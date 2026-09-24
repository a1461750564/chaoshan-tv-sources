#!/usr/bin/env python3
"""
直播源自动更新流水线
=====================

做什么
------
1. 读工程里已有的 assets/builtin_iptv.txt 作为「骨架」——频道顺序、潮汕的 sttv://
   动态源、官方 webview 兜底，这些**原样保留**。
2. 把骨架里的 http 直连条目 + 从多个公开聚合源抓来的候选，一起做三级检测。
3. 用检测结果替换骨架里的 http 直连部分，输出新的播放列表。

这样频道顺序和稳定条目（汕头动态源、官方网页）不会被流水线破坏，
只有会过期的那部分（公开 http 直连）被定期换新。

三类条目的处理方式
------------------
- **公开直连（http）**：会过期，每次跑都重新检测换新，**排在第一位**。
- **钉住（PINNED_HOSTS）**：央视官方 CDN（网宿/腾讯/快手/百度/火山）。
  不带鉴权 token、长期有效，**永不替换**，但**排在公开源之后**作为后备层
  ——原因见 [is_pinned] 上方注释（官方源 SPS 异常，在 Android 9 上会整屏纯绿
  且不触发 onError，排第一会卡死）。单独、低并发、带 3 次重试地复检
  （不能和大批候选一起高并发扫——实测那样会有约 20% 的假阴性）；
  即使复检仍失败也**保留不删**，只打印告警让人确认。
- **稳定（stable）**：`sttv://` 客户端签名源、`webview://` 官方网页兜底。原样保留，排最后。

用法
----
    python3 tools/update_sources.py                  # 原地更新 assets/builtin_iptv.txt
    python3 tools/update_sources.py -o /tmp/live.txt # 输出到别处
    python3 tools/update_sources.py --dry-run        # 只报告，不写文件

只依赖标准库，GitHub Actions 无需 pip install。
"""

import argparse, json, os, re, socket, ssl, sys, time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlsplit, quote
import urllib.request

ssl._create_default_https_context = ssl._create_unverified_context
socket.setdefaulttimeout(12)

UA = "Mozilla/5.0 (Linux; Android 11) ExoPlayer"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PLAYLIST = os.path.join(ROOT, "tv", "src", "main", "assets", "builtin_iptv.txt")

# ── 候选来源：优先取「自带校验/测速」的成品源 ────────────────────────
# 路径里含中文的必须 URL 编码，否则 urllib 会抛 UnicodeEncodeError
def _enc(u: str) -> str:
    scheme, _, rest = u.partition("://")
    host, _, path = rest.partition("/")
    return f"{scheme}://{host}/" + quote(path) if path else u


AGGREGATORS = [
    ("TV1-ipv4",      "https://raw.githubusercontent.com/xiongjian83/TV1/HEAD/output/ipv4/result.m3u"),
    ("TV1-all",       "https://raw.githubusercontent.com/xiongjian83/TV1/HEAD/output/result.m3u"),
    ("best-fan",      "https://raw.githubusercontent.com/best-fan/iptv-sources/HEAD/cn_all.m3u8"),
    ("best-fan-cctv", "https://raw.githubusercontent.com/best-fan/iptv-sources/HEAD/cn_cctv.m3u8"),
    ("akiralereal",   "https://raw.githubusercontent.com/akiralereal/iptv/HEAD/IPTV.m3u"),
    ("CCSH-lite",     "https://raw.githubusercontent.com/CCSH/IPTV/HEAD/live_lite.m3u"),
    ("hujingguang",   "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/HEAD/cnTV1_ALL.m3u8"),
    ("zhi35",         "https://raw.githubusercontent.com/zhi35/iptv/HEAD/iptv.m3u"),
    ("kilvn",         "https://raw.githubusercontent.com/kilvn/iptv/HEAD/iptv.m3u"),
    ("Meroser",       "https://raw.githubusercontent.com/Meroser/IPTV/HEAD/IPTV-demo.m3u"),
    ("suxuang",       "https://raw.githubusercontent.com/suxuang/myIPTV/HEAD/移动IPTV.m3u"),
    ("xisohi",        "https://raw.githubusercontent.com/xisohi/CHINA-IPTV/HEAD/TV/live.txt"),
    ("nthack-All",    "https://raw.githubusercontent.com/nthack/IPTVM3U/HEAD/All.m3u"),
    ("vbskycn",       "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt"),
    ("ssili126",      "https://raw.githubusercontent.com/ssili126/tv/main/temp/IPTV.txt"),
    ("ibert",         "https://m3u.ibert.me/txt/fmml_ipv6.txt"),
]

# 每频道最多保留几条 http 直连（多了换台试错慢）
MAX_DIRECT = 4
# 收集阶段每频道最多攒多少条候选（宽进严出：先多收，检测后再择优）
MAX_CANDIDATES = 40

CCTV = [f"CCTV{i}" for i in range(1, 18)] + ["CCTV5+"]
SAT = ["北京卫视","湖南卫视","浙江卫视","江苏卫视","东方卫视","安徽卫视","山东卫视",
       "广东卫视","深圳卫视","天津卫视","河北卫视","河南卫视","湖北卫视","四川卫视",
       "重庆卫视","辽宁卫视","黑龙江卫视","吉林卫视","陕西卫视","山西卫视","江西卫视",
       "东南卫视","贵州卫视","云南卫视","广西卫视","甘肃卫视","青海卫视","宁夏卫视",
       "新疆卫视","西藏卫视","内蒙古卫视","海南卫视"]


# ── 频道名归一化 ─────────────────────────────────────────────────────
def norm(name: str) -> str:
    n = name.strip()
    n = re.sub(r"\s*[（(].*?[)）]\s*$", "", n)
    n = re.sub(r"[-_ ]?(高清|超清|标清|HD|SD|FHD|4K|蓝光|流畅)+$", "", n, flags=re.I)
    n = n.replace("中央电视台", "CCTV").replace("央视", "CCTV")
    m = re.match(r"^CCTV[- ]?(\d+\+?)", n, flags=re.I)
    if m:
        return "CCTV" + m.group(1)
    alias = {"CCTV综合":"CCTV1","CCTV经济":"CCTV2","CCTV综艺":"CCTV3","CCTV中文国际":"CCTV4",
             "CCTV体育":"CCTV5","CCTV电影":"CCTV6","CCTV国防军事":"CCTV7","CCTV科教":"CCTV10",
             "CCTV戏曲":"CCTV11","CCTV社会与法":"CCTV12","CCTV新闻":"CCTV13","CCTV少儿":"CCTV14",
             "CCTV音乐":"CCTV15","CCTV农业":"CCTV17"}
    return alias.get(n, n)


def clean_url(u: str):
    """过滤脏 URL（来源文件里有用 ';' 当分隔符导致拼接错乱的条目）"""
    u = u.strip()
    if not u.startswith("http"):
        return None
    if ";" in u or re.search(r"\.m3u8\w+$", u):
        return None
    return u


# ── 钉住：央视官方 CDN ───────────────────────────────────────────────
# 这些是央视网自有的分发地址（CCTV-1..17 各 5 路），不带鉴权 token、不限时，
# 实测长期有效。各域名对应的 CDN：网宿 / 腾讯云 / 快手 / 百度 / 火山引擎。
#
# ⚠️ 为什么它们排在公开源之后、只当后备层
# ----------------------------------------
# 实测（Android 9 / API 28 模拟器，Media3 与 IJK 两个播放器结果一致）：
# 官方源整屏纯绿（RGB 均值约 1/118/1 —— 只有亮度平面有数据，色度平面未写入），
# 而同一环境下的公开源、河北卫视、浙江卫视 HTTPS、Apple 测试流全部正常。
# 逐字段解析 H.264 SPS 后定位到差异：官方源 pic_order_cnt_type = 3，
# 是规范保留值（合法值只有 0/1/2），解码器行为未定义；能正常播的源分别是
# 2 / 0 / 0。更麻烦的是绿屏**不会触发播放器 onError**，App 的多线路自动切换
# 因此失效——若把它排在第一位，一旦复现就是"央视永远绿屏且无人能救"。
# 故降级为后备：公开源全部失效时才轮到它（届时绿屏也比什么都没有强）。
PINNED_HOSTS = (
    ".wscdns.com",              # 网宿
    ".liveplay.myqcloud.com",   # 腾讯云
    ".v.kcdnvip.com",           # 快手
    ".a.bdydns.com",            # 百度
    ".volcfcdn.com",            # 火山引擎
)


def is_pinned(u: str) -> bool:
    try:
        host = urlsplit(u).hostname or ""
    except ValueError:
        return False
    return any(host.endswith(h) for h in PINNED_HOSTS)


def host_of(u: str) -> str:
    """取 URL 的 host[:port]，用于归属地判断"""
    try:
        return urlsplit(u).netloc
    except ValueError:
        return ""


# ── 三级检测 ─────────────────────────────────────────────────────────
def _get(url, timeout=10, limit=None):
    r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=timeout)
    return r.status, r.headers.get("Content-Type", ""), (r.read() if limit is None else r.read(limit))


# PMT 里代表「有画面」的 stream_type
# 0x01 MPEG-1 / 0x02 MPEG-2 / 0x10 MPEG-4 / 0x1B H.264 / 0x1E H.264-MVC
# 0x24 H.265 / 0x42 AVS / 0xD1 Dirac / 0xEA VC-1
_VIDEO_STREAM_TYPES = {0x01, 0x02, 0x10, 0x1B, 0x1E, 0x24, 0x42, 0xD1, 0xEA}


def ts_has_video(data: bytes):
    """从 TS 分片判断有没有画面。

    返回 True=有视频 / False=纯音频 / None=判不了（非 TS，保守放行）

    为什么要查这个：公开源里混着一批 `…/audio/cctv3_2.m3u8` 这类**纯音频**地址，
    它们能通过前面三级检测（连通、真 HLS、首片能拉），但播出来是**黑屏只有声音**。
    实测 CCTV16 的第 1 条就是这种，会直接顶掉真正的视频线路。
    """
    pkts = [data[i:i + 188] for i in range(0, len(data) - 187, 188)]
    pkts = [p for p in pkts if p[0] == 0x47]
    if len(pkts) < 10:
        return None

    def payload(p):
        af = (p[3] >> 4) & 0x3
        q = (5 + p[4]) if af in (2, 3) else 4
        if q >= len(p):
            return None
        return p[q + 1 + p[q]:] if p[q] < len(p) else None

    pmt_pid = None
    for p in pkts:                                                  # PAT → PMT PID
        if ((p[1] & 0x1f) << 8 | p[2]) == 0 and (p[1] & 0x40):
            s = payload(p)
            if s and s[0] == 0x00:
                end = 3 + (((s[1] & 0xf) << 8) | s[2]) - 4
                for i in range(8, min(end, len(s) - 4), 4):
                    if ((s[i] << 8) | s[i + 1]) != 0:
                        pmt_pid = ((s[i + 2] & 0x1f) << 8) | s[i + 3]
                        break
            break
    if pmt_pid is None:
        return None

    for p in pkts:                                                  # PMT → 流类型表
        if ((p[1] & 0x1f) << 8 | p[2]) == pmt_pid and (p[1] & 0x40):
            s = payload(p)
            if s and s[0] == 0x02:
                end = 3 + (((s[1] & 0xf) << 8) | s[2]) - 4
                i = 12 + (((s[10] & 0xf) << 8) | s[11])
                types = []
                while i + 5 <= min(end, len(s)):
                    types.append(s[i])
                    i += 5 + (((s[i + 3] & 0xf) << 8) | s[i + 4])
                if types:
                    return any(t in _VIDEO_STREAM_TYPES for t in types)
            break
    return None


def first_segment(url, text, depth=0):
    """拉取「首个真正能播的片段」，返回 (url, content_type, 数据) 或 None。

    很多源是**主播放列表**（外层 m3u8 列的是不同码率的子播放列表，子播放列表才是切片）。
    只下钻一层、把子播放列表当切片去拉，拿回来还是 m3u8，就会被误判成"拉不到"。
    实测广东的源里大量是这种结构（`epg.pw`、`jdshipin` 等），
    修掉它对所有频道的候选命中率都有帮助，不只是广东。

    depth 限制 2 层，防止自引用导致死循环。
    """
    segs = [l.strip() for l in text.splitlines()
            if l.strip() and not l.startswith("#")]
    if not segs or len(segs) > 300:                                 # 分片数合理
        return None                                                 # 超过 300 基本是点播切片
    seg_url = urljoin(url, segs[0])
    st, ct, body = _get(seg_url, 12, 65536)
    if st != 200:
        return None
    if body.lstrip()[:7] == b"#EXTM3U" and depth < 2:               # 还是播放列表 → 下钻
        return first_segment(seg_url, body.decode("utf-8", "ignore"), depth + 1)
    return seg_url, ct, body


def probe(item):
    """通过返回 (频道名, url)，否则 None

    只看 HTTP 200 会得到约 97% 的假可用率 —— 必须验到分片一级。
    """
    name, url = item
    try:
        st, _ct, body = _get(url, 10, 200000)                       # ① 连通性
        text = body.decode("utf-8", "ignore").lstrip()
        if not text.startswith("#EXTM3U"):                          # ② 必须是真的 HLS
            return None                                             #    否则可能是 MP4（首行 ftypqt）
        got = first_segment(url, text)                              # ③ 首分片真能拉（自动下钻主播放列表）
        if got is None:
            return None
        seg_url, ct2, seg = got
        if "/audio/" in url.lower() or ct2.startswith("audio"):      # ④ 明摆着的音频地址
            return None
        # ⑤ 要有画面。注意判据是「TS 同步字节 + PMT」，不认 content-type ——
        #    不少源把 TS 分片标成 text/plain 或干脆不给 type，卡 content-type 会误杀。
        if seg[:1] == b"\x47" or ct2.startswith("video") or ct2.startswith("octet"):
            if ts_has_video(seg) is False:
                return None                                         #    PMT 里只有音频流
            return (name, url)
    except Exception:
        pass
    return None


def probe_pinned(item, tries=3):
    """钉住源专用探测：带重试

    实测教训——在 40 线程扫 2000 条候选时，官方 CDN 会因自身限流/瞬时超时
    产生约 20% 的假阴性（实测 18 条报「失效」，单独复检 18 条全活）。
    钉住源是长期资产，绝不能因一次抖动被误删，所以低并发 + 多次重试。
    """
    for i in range(tries):
        r = probe(item)
        if r is not None:
            return r
        if i < tries - 1:
            time.sleep(1.5 * (i + 1))
    return None


# ── 主机归属地：把境外转播源降到最后 ─────────────────────────────────
GEO_API = "http://ip-api.com/batch?fields=query,countryCode"


def find_foreign_hosts(hosts):
    """返回其中的**境外**主机集合。查不到就返回空集（best-effort，不阻塞主流程）。

    为什么要这一步
    --------------
    公开源里混着境外的转播服务器。实测整个播放列表 61 个主机里只有一个是境外的
    —— `74.91.26.218:82`（美国密苏里 Nocix 机房，org "Chengdu Zhimeng"），
    而一台机器就转发了 31 个央视频道（cctv1hd ~ cctv17hd）。
    它恰好被排在了 **CCTV1 和 CCTV9 的第 1 条线路**上。

    对国内用户的坏处有两层：
    1. **信号可能不是国内版**。境外转播常见的是「海外版」信号，广告甚至节目都与
       国内版不同 —— 用户实测反馈「CCTV1 第一个源是个广告」，与此吻合。
    2. **链路绕远**。视频要从国内传到美国再拉回来，延迟和稳定性都差。

    所以降级到列表末尾：留着当最后的兜底，但永不优先。
    """
    ip_of = {}
    for h in hosts:
        try:
            ip_of[h] = socket.gethostbyname(h.split(":")[0])
        except Exception:
            continue
    uniq = sorted(set(ip_of.values()))
    if not uniq:
        return set()
    foreign_ips = set()
    for i in range(0, len(uniq), 100):                      # API 每批上限 100
        try:
            req = urllib.request.Request(
                GEO_API,
                data=json.dumps(uniq[i:i + 100]).encode(),
                headers={"Content-Type": "application/json"},
            )
            for it in json.loads(urllib.request.urlopen(req, timeout=20).read().decode()):
                code = it.get("countryCode")
                if code and code != "CN":
                    foreign_ips.add(it.get("query"))
        except Exception as e:
            print(f"  [归属地] 查询失败（跳过，不影响主流程）: {type(e).__name__}",
                  file=sys.stderr)
            return set()
    return {h for h, ip in ip_of.items() if ip in foreign_ips}


# ── 骨架解析 / 生成 ──────────────────────────────────────────────────
def read_skeleton(path):
    """返回 [(分组名 or None, 频道名, [线路...])]，顺序与文件一致"""
    out = []
    group = None
    if not os.path.exists(path):
        return out
    for raw in open(path, encoding="utf-8", errors="ignore"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "#genre#" in line:
            group = line.split(",")[0].strip()
            continue
        name, _, urls = line.partition(",")
        out.append((group, name.strip(), [u for u in urls.split("#") if u]))
    return out


def collect_candidates(skeleton, target_names):
    """候选 = 骨架里的 http 直连（重新测） + 聚合源抓来的"""
    pool = OrderedDict((n, []) for n in target_names)
    for _g, name, urls in skeleton:
        if name not in pool:
            continue
        for u in urls:
            if u.startswith("http"):
                c = clean_url(u)
                if c and c not in pool[name]:
                    pool[name].append(c)

    for tag, url in AGGREGATORS:
        try:
            req = urllib.request.Request(_enc(url), headers={"User-Agent": UA})
            text = urllib.request.urlopen(req, timeout=40).read().decode("utf-8", "ignore")
        except Exception as e:
            print(f"  [{tag}] 抓取失败 {type(e).__name__}: {e}", file=sys.stderr)
            continue

        got = 0
        if text.lstrip().startswith("#EXTM3U"):
            lines = text.splitlines()
            for i, l in enumerate(lines):
                if not l.startswith("#EXTINF"):
                    continue
                disp = l.rsplit(",", 1)[-1].strip()
                m = re.search(r'tvg-name="([^"]+)"', l)
                name = norm(m.group(1) if m else disp)
                if name not in pool or len(pool[name]) >= MAX_CANDIDATES:
                    continue
                for j in range(i + 1, min(i + 8, len(lines))):
                    nx = lines[j].strip()
                    if not nx or nx.startswith("#"):
                        continue
                    c = clean_url(nx)
                    if c and c not in pool[name]:
                        pool[name].append(c)
                        got += 1
                    break
        else:
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("//") or "#genre#" in line:
                    continue
                p = re.split(r"[,，]", line, maxsplit=1)
                if len(p) < 2:
                    continue
                name = norm(p[0])
                if name not in pool or len(pool[name]) >= MAX_CANDIDATES:
                    continue
                for u in p[1].split("#"):
                    c = clean_url(u)
                    if c and c not in pool[name]:
                        pool[name].append(c)
                        got += 1
                        break
        print(f"  [{tag}] 新增 {got} 条候选", file=sys.stderr)

    return pool


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--playlist", default=DEFAULT_PLAYLIST, help="骨架播放列表路径")
    ap.add_argument("-o", "--out", help="输出路径（默认原地更新）")
    ap.add_argument("--dry-run", action="store_true", help="只报告不写文件")
    ap.add_argument("--workers", type=int, default=40)
    args = ap.parse_args()

    skeleton = read_skeleton(args.playlist)
    if not skeleton:
        print(f"错误: 读不到骨架 {args.playlist}", file=sys.stderr)
        return 1

    all_names = [n for _g, n, _u in skeleton]
    print(f"骨架: {len(skeleton)} 个频道", file=sys.stderr)

    print("收集候选...", file=sys.stderr)
    pool = collect_candidates(skeleton, set(all_names))

    todo = [(n, u) for n, us in pool.items() for u in us]
    print(f"三级检测 {len(todo)} 条候选（{args.workers} 线程）...", file=sys.stderr)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        results = [r for r in ex.map(probe, todo) if r]
    print(f"通过 {len(results)} 条，耗时 {time.time()-t0:.0f}s", file=sys.stderr)

    # 钉住的官方 CDN 单独、低并发、带重试地复检。
    # 不混在上面那批里——高并发会让官方 CDN 限流，产生约 20% 的假阴性。
    pinned_todo = [(n, u) for _g, n, us in skeleton for u in us if is_pinned(u)]
    dead_pinned = []
    if pinned_todo:
        print(f"复检钉住的官方 CDN {len(pinned_todo)} 条（8 线程 × 3 次重试）...", file=sys.stderr)
        with ThreadPoolExecutor(max_workers=8) as ex:
            for (n, u), r in zip(pinned_todo, ex.map(probe_pinned, pinned_todo)):
                if r is None:
                    dead_pinned.append(u)
        print(f"官方 CDN 复检完成：{len(pinned_todo)-len(dead_pinned)} 条存活", file=sys.stderr)

    fresh = OrderedDict()
    for n, u in results:
        fresh.setdefault(n, [])
        if len(fresh[n]) < MAX_DIRECT:
            fresh[n].append(u)

    # 查一遍主机归属地，把境外转播源降到最后（详见 find_foreign_hosts 注释）
    all_hosts = {host_of(u) for n, u in results}
    all_hosts |= {host_of(u) for _g, _n, us in skeleton for u in us
                  if u.startswith("http")}
    all_hosts.discard("")
    print(f"检查 {len(all_hosts)} 个主机的归属地...", file=sys.stderr)
    FOREIGN = find_foreign_hosts(all_hosts)
    if FOREIGN:
        print(f"  境外主机 {len(FOREIGN)} 个（将排到各频道最后）: "
              f"{', '.join(sorted(FOREIGN))}", file=sys.stderr)
    else:
        print("  全部境内（或查询失败）", file=sys.stderr)

    # ── 生成：骨架顺序不变，http 部分换成新测的，非 http 部分原样保留 ──
    out_lines = []
    last_group = None
    stats = {"fresh": 0, "stable": 0, "pinned": 0, "direct_channels": 0, "overseas": 0}

    for group, name, urls in skeleton:
        if group != last_group:
            if last_group is not None:
                out_lines.append("")
            out_lines.append(f"{group},#genre#")
            last_group = group

        passed = set(fresh.get(name, []))

        # 钉住源一律保留，**包括复检失败的**：
        # App 多线路失败会自动切换，多留一条死线路的代价，
        # 远小于因瞬时网络抖动误删一条好线路（实测假阴性率约 20%）。
        pinned = [u for u in urls if is_pinned(u)]

        stable = [u for u in urls if not u.startswith("http")]   # sttv:// / webview://
        skeleton_http = [u for u in urls if u.startswith("http") and not is_pinned(u)]

        # 所有通过三级检测的 http 线路合并去重：新测的优先，骨架里测活过的补位
        leftover = [u for u in skeleton_http if u in passed]
        merged_http = []
        for u in fresh.get(name, []) + leftover:
            if u not in merged_http:
                merged_http.append(u)
        # 公开聚合源里已经出现了官方 CDN 地址（实测 best-fan 等已收录 wscdns），
        # 必须把它们从「公开段」剔除 —— 否则又会被排到第一位，回到绿屏卡死的老问题。
        # 它们由 pinned 段统一承载，位置在后备层。
        merged_http = [u for u in merged_http if not is_pinned(u)]

        # 境外的排到最后：只当兜底，永不优先（详见 find_foreign_hosts 注释）
        domestic = [u for u in merged_http if host_of(u) not in FOREIGN][:MAX_DIRECT]
        overseas = [u for u in merged_http if host_of(u) in FOREIGN]

        # 顺序 = 境内公开源 → 钉住的官方 CDN → 固定条目(sttv/webview) → 境外转播源
        #
        # 官方 CDN 不排第一是有实测依据的：央视官方源的 H.264 SPS 里
        # pic_order_cnt_type = 3（保留值，规范只允许 0/1/2），Android 9 模拟器的
        # 软解（Media3 与 IJK 都一样）只能解出亮度平面，整屏纯绿、且不触发 onError
        # ——App 不会自动切走。同一环境下公开源与其它台都正常。
        # 故官方源降级为「公开源全挂时」的后备层，避免绿屏卡死无人能救。
        merged = []
        for u in domestic + pinned + stable + overseas:
            if u not in merged:
                merged.append(u)
        if not merged:
            print(f"  跳过 {name}（无任何可用线路）", file=sys.stderr)
            continue

        out_lines.append(f"{name}," + "#".join(merged))
        if merged_http:
            stats["direct_channels"] += 1
        stats["pinned"] += len(pinned)
        stats["fresh"] += len(domestic)
        stats["overseas"] += len(overseas)
        stats["stable"] += len(stable)

    text = "\n".join(out_lines) + "\n"

    print(f"\n结果: {stats['pinned']} 条官方 CDN，"
          f"{stats['direct_channels']} 个频道有公开 http 直连（境内 {stats['fresh']} 条"
          + (f" / 境外 {stats['overseas']} 条" if stats["overseas"] else "")
          + f"），固定条目 {stats['stable']} 条", file=sys.stderr)

    if dead_pinned:
        print(f"\n提示：{len(dead_pinned)} 条官方 CDN 三次重试后仍不可达（已保留，未删除）。", file=sys.stderr)
        print(f"      若本流水线跑在 GitHub 海外机房，这是正常现象 —— 部分国内 CDN 节点从境外"
              f"连不上，但在国内可达（实测云端约 75/90、本地约 90/90）。", file=sys.stderr)
        print(f"      只有在**国内网络**下也大面积不可达时，才说明央视 CDN 真有变动，"
              f"届时从新版 tv-go 镜像的 cctv_streams.json 抄一批新地址:", file=sys.stderr)
        for u in dead_pinned:
            print(f"    ✗ {u}", file=sys.stderr)

    if args.dry_run:
        print("--dry-run，未写文件", file=sys.stderr)
        return 0

    out_path = args.out or args.playlist
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"已写出: {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
