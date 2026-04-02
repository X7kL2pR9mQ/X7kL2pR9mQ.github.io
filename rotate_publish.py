#!/usr/bin/env python3
"""Upload videos in small batches until fully published."""

from __future__ import annotations

import argparse
import html
import json
import re
import shutil
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = REPO_ROOT.parent
STATE_FILE = REPO_ROOT / ".upload_state.json"
GOLDEN_PATH = PROJECT_ROOT / "golden_set_0401.json"

DATASETS = {
    "skyreels_multi": {
        "label": "SkyReels V3 多图R2V",
        "source_dir": PROJECT_ROOT / "skyreels_golden" / "videos_3ref_step8_5s_guide2_0331prompt",
        "asset_dir": REPO_ROOT / "assets" / "skyreels_multi",
        "pattern": re.compile(r"^(M900[0-3])__prompt(\d+)__5s\.mp4$"),
    },
    "skyreels_single": {
        "label": "SkyReels V3 单图R2V",
        "source_dir": PROJECT_ROOT / "skyreels_golden" / "videos_1ref_step8_5s_guide2_0401prompt",
        "asset_dir": REPO_ROOT / "assets" / "skyreels_single",
        "pattern": re.compile(r"^(M900[0-3])__prompt(\d+)__5s\.mp4$"),
    },
    "veo3": {
        "label": "veo3-1fast",
        "source_dir": PROJECT_ROOT / "veo3-1fast_golden",
        "asset_dir": REPO_ROOT / "assets" / "veo3",
        "pattern": re.compile(r"^(M900[0-3])__multiref__prompt(\d+)__8s\.mp4$"),
    },
    "wan26": {
        "label": "Wan 2.6",
        "source_dir": PROJECT_ROOT / "wan2-6_golden",
        "asset_dir": REPO_ROOT / "assets" / "wan26",
        "pattern": re.compile(r"^(M900[0-3])__prompt(\d+)__5s\.mp4$"),
    },
}

DATASET_KEYS: tuple[str, ...] = ("skyreels_multi", "skyreels_single", "veo3", "wan26")
ACTIVE_DATASET_KEY = "skyreels_multi"

GROUP_THEME = {
    0: {"accent": "#0d9488", "bg": "rgba(13, 148, 136, 0.08)"},
    1: {"accent": "#2563eb", "bg": "rgba(37, 99, 235, 0.08)"},
    2: {"accent": "#7c3aed", "bg": "rgba(124, 58, 237, 0.08)"},
    3: {"accent": "#d97706", "bg": "rgba(217, 119, 6, 0.1)"},
}


DEFAULT_STATE = {"skyreels_multi": 0, "skyreels_single": 0, "veo3": 0, "wan26": 0}


def load_state() -> dict[str, int]:
    if not STATE_FILE.exists():
        return dict(DEFAULT_STATE)
    try:
        raw = STATE_FILE.read_text(encoding="utf-8").strip()
        if not raw:
            return dict(DEFAULT_STATE)
        data = json.loads(raw)
        if "skyreels" in data and "skyreels_multi" not in data:
            data["skyreels_multi"] = data["skyreels"]
        if "skyreels_single" not in data:
            data["skyreels_single"] = 0
        return {k: int(data.get(k, 0)) for k in DEFAULT_STATE}
    except Exception:
        return dict(DEFAULT_STATE)


def save_state(state: dict[str, int]) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def list_videos(dataset_key: str) -> list[tuple[str, int, Path]]:
    conf = DATASETS[dataset_key]
    out: list[tuple[str, int, Path]] = []
    for p in sorted(conf["source_dir"].iterdir(), key=lambda x: x.name):
        if not p.is_file() or p.suffix.lower() != ".mp4":
            continue
        m = conf["pattern"].match(p.name)
        if not m:
            continue
        mid, pid_s = m.group(1), m.group(2)
        out.append((mid, int(pid_s), p))
    out.sort(key=lambda x: (x[1], x[0], x[2].name))
    return out


def pick_batch(items: list[tuple[str, int, Path]], start: int, size: int) -> tuple[list[tuple[str, int, Path]], int]:
    if not items:
        return [], 0
    n = len(items)
    idx = max(0, min(start, n))
    end = min(idx + size, n)
    return items[idx:end], end


def append_assets(
    dataset_key: str,
    chosen: list[tuple[str, int, Path]],
    remote_assets: set[str],
    *,
    ignore_remote_skip: bool = False,
) -> tuple[int, int]:
    conf = DATASETS[dataset_key]
    asset_dir: Path = conf["asset_dir"]
    asset_dir.mkdir(parents=True, exist_ok=True)
    added = 0
    skipped_remote = 0
    for mid, pid, src in chosen:
        dst = asset_dir / src.name
        rel = dst.relative_to(REPO_ROOT).as_posix()
        if not ignore_remote_skip and rel in remote_assets:
            skipped_remote += 1
            continue
        shutil.copy2(src, dst)
        added += 1
    return added, skipped_remote


def get_remote_asset_set() -> set[str]:
    try:
        out = subprocess.check_output(
            ["git", "ls-tree", "-r", "--name-only", "origin/main"],
            cwd=REPO_ROOT,
            text=True,
        )
    except Exception:
        return set()
    return {
        line.strip()
        for line in out.splitlines()
        if line.startswith("assets/") and line.endswith(".mp4")
    }


def collect_asset_items(dataset_key: str) -> list[tuple[str, int, str]]:
    conf = DATASETS[dataset_key]
    out: list[tuple[str, int, str]] = []
    asset_dir: Path = conf["asset_dir"]
    asset_dir.mkdir(parents=True, exist_ok=True)
    for p in sorted(asset_dir.iterdir(), key=lambda x: x.name):
        if not p.is_file() or p.suffix.lower() != ".mp4":
            continue
        m = conf["pattern"].match(p.name)
        if not m:
            continue
        mid, pid_s = m.group(1), m.group(2)
        out.append((mid, int(pid_s), p.relative_to(REPO_ROOT).as_posix()))
    out.sort(key=lambda x: (x[1], x[0], x[2]))
    return out


def group_by_id(items: list[tuple[str, int, str]]) -> dict[int, list[tuple[int, str]]]:
    grouped: dict[int, list[tuple[int, str]]] = {i: [] for i in range(4)}
    for mid, pid, rel in items:
        gid = int(mid[-1])
        grouped[gid].append((pid, rel))
    for gid in grouped:
        grouped[gid].sort(key=lambda x: x[0])
    return grouped


def build_dataset_section(
    dataset_key: str, label: str, grouped: dict[int, list[tuple[int, str]]], golden: dict[str, str], *, is_active: bool
) -> str:
    active_cls = " is-active" if is_active else ""
    parts = [f'<section class="dataset{active_cls}" data-dataset="{dataset_key}">']
    parts.append('<nav class="toc" aria-label="分组跳转">')
    for gid in range(4):
        n = len(grouped[gid])
        parts.append(
            f'<a class="toc-link" href="#{dataset_key}-ID{gid}" style="--accent:{GROUP_THEME[gid]["accent"]}">'
            f'ID {gid} <span class="count">({n})</span></a>'
        )
    parts.append("</nav>")
    for gid in range(4):
        theme = GROUP_THEME[gid]
        parts.append(
            f'<section id="{dataset_key}-ID{gid}" class="group" style="--accent:{theme["accent"]};--group-bg:{theme["bg"]}">'
        )
        parts.append(f'<header class="group-header"><h2>ID {gid}</h2>')
        parts.append(f'<span class="group-meta">{label} · 已累计上传 {sum(len(v) for v in grouped.values())} 个</span></header>')
        parts.append('<div class="grid">')
        for pid, rel in grouped[gid]:
            prompt = html.escape(golden.get(str(pid), "（golden_set_0401.json 中无此 id）"))
            parts.append('<article class="card">')
            parts.append(
                f'<div class="video-wrap"><video controls preload="none" playsinline data-src="{html.escape(rel)}"></video></div>'
            )
            parts.append(f'<div class="meta"><span class="badge">prompt {pid}</span><p class="prompt">{prompt}</p></div>')
            parts.append("</article>")
        parts.append("</div></section>")
    parts.append("</section>")
    return "".join(parts)


def write_index(
    grouped: dict[str, dict[int, list[tuple[int, str]]]],
    golden: dict[str, str],
) -> None:
    html_out = [HTML_HEAD]
    opts = []
    for key in DATASET_KEYS:
        lab = html.escape(DATASETS[key]["label"])
        sel = " selected" if key == ACTIVE_DATASET_KEY else ""
        opts.append(f'<option value="{html.escape(key)}"{sel}>{lab}</option>')
    html_out.append(
        '<div class="source-switch"><label for="sourceSelect">数据源</label>'
        '<select id="sourceSelect" aria-label="选择数据源">'
        + "".join(opts)
        + "</select></div>"
    )
    for key in DATASET_KEYS:
        html_out.append(
            build_dataset_section(
                key, DATASETS[key]["label"], grouped[key], golden, is_active=(key == ACTIVE_DATASET_KEY)
            )
        )
    html_out.append(HTML_TAIL)
    (REPO_ROOT / "index.html").write_text("".join(html_out), encoding="utf-8")


def run_git(*args: str) -> None:
    subprocess.run(["git", *args], cwd=REPO_ROOT, check=True)


def _push_output() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "push", "-u", "origin", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def git_push_with_retry(max_retries: int, retry_wait: float) -> None:
    last_err: Exception | None = None
    i = 0
    rebase_attempts = 0
    max_rebase = 8
    while max_retries == 0 or i < max_retries:
        r = _push_output()
        if r.returncode == 0:
            return
        combined = (r.stderr or "") + (r.stdout or "")
        if "non-fast-forward" in combined or "! [rejected]" in combined:
            rebase_attempts += 1
            if rebase_attempts > max_rebase:
                raise RuntimeError(
                    "git push still non-fast-forward after repeated pull --rebase; resolve conflicts manually."
                ) from None
            print("Remote main is ahead; running git pull --rebase origin main ...")
            rb = subprocess.run(
                ["git", "pull", "--rebase", "origin", "main"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
            if rb.returncode != 0:
                raise RuntimeError(
                    "git pull --rebase failed (conflict or network). Fix locally, then re-run.\n"
                    + (rb.stderr or rb.stdout or "")
                ) from None
            time.sleep(0.5)
            continue
        last_err = RuntimeError(combined.strip() or f"git push exit {r.returncode}")
        i += 1
        if max_retries != 0 and i >= max_retries:
            break
        backoff_idx = min(i - 1, 10)
        wait_s = retry_wait * (2**backoff_idx)
        total_text = "infinite" if max_retries == 0 else str(max_retries)
        print(f"Push failed (attempt {i}/{total_text}), retry in {wait_s:.1f}s...")
        time.sleep(wait_s)
    raise RuntimeError("git push failed after retries") from last_err


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=4, help="Each dataset batch size, default 4.")
    parser.add_argument("--commit", action="store_true", help="Commit generated changes.")
    parser.add_argument("--push", action="store_true", help="Push after commit (implies --commit).")
    parser.add_argument("--max-retries", type=int, default=0, help="Max git push retries; 0 means infinite.")
    parser.add_argument("--retry-wait", type=float, default=2.0, help="Initial seconds between retries.")
    parser.add_argument("--until-done", action="store_true", help="Keep batching until all videos are uploaded.")
    parser.add_argument(
        "--regenerate-index",
        action="store_true",
        help="Only rewrite index.html from current assets (no upload state change).",
    )
    parser.add_argument(
        "--ignore-remote-skip",
        action="store_true",
        help="Copy even when origin/main already has the asset path (for replacing same-named files).",
    )
    args = parser.parse_args()

    if args.batch_size <= 0 or args.max_retries < 0 or args.retry_wait <= 0:
        raise SystemExit("batch-size>0, max-retries>=0, retry-wait>0")

    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))

    if args.regenerate_index:
        grouped = {key: group_by_id(collect_asset_items(key)) for key in DATASET_KEYS}
        write_index(grouped, golden)
        print("Wrote index.html only.")
        return

    state = load_state()

    round_idx = 0
    while True:
        round_idx += 1
        run_git("fetch", "origin", "main")
        remote_assets = get_remote_asset_set()
        sky_m_all = list_videos("skyreels_multi")
        sky_s_all = list_videos("skyreels_single")
        veo_all = list_videos("veo3")
        wan_all = list_videos("wan26")
        sky_m_batch, next_sky_m = pick_batch(sky_m_all, state["skyreels_multi"], args.batch_size)
        sky_s_batch, next_sky_s = pick_batch(sky_s_all, state["skyreels_single"], args.batch_size)
        veo_batch, next_veo = pick_batch(veo_all, state["veo3"], args.batch_size)
        wan_batch, next_wan = pick_batch(wan_all, state["wan26"], args.batch_size)

        sky_m_added, sky_m_skipped = append_assets(
            "skyreels_multi", sky_m_batch, remote_assets, ignore_remote_skip=args.ignore_remote_skip
        )
        sky_s_added, sky_s_skipped = append_assets(
            "skyreels_single", sky_s_batch, remote_assets, ignore_remote_skip=args.ignore_remote_skip
        )
        veo_added, veo_skipped = append_assets(
            "veo3", veo_batch, remote_assets, ignore_remote_skip=args.ignore_remote_skip
        )
        wan_added, wan_skipped = append_assets(
            "wan26", wan_batch, remote_assets, ignore_remote_skip=args.ignore_remote_skip
        )
        state = {
            "skyreels_multi": next_sky_m,
            "skyreels_single": next_sky_s,
            "veo3": next_veo,
            "wan26": next_wan,
        }

        grouped = {key: group_by_id(collect_asset_items(key)) for key in DATASET_KEYS}
        write_index(grouped, golden)

        print(
            f"[Round {round_idx}] added sky_multi={sky_m_added}, sky_single={sky_s_added}, "
            f"veo3={veo_added}, wan26={wan_added}; "
            f"skipped(remote) sky_multi={sky_m_skipped}, sky_single={sky_s_skipped}, "
            f"veo3={veo_skipped}, wan26={wan_skipped}"
        )
        print(
            f"[Round {round_idx}] progress sky_multi={next_sky_m}/{len(sky_m_all)}, "
            f"sky_single={next_sky_s}/{len(sky_s_all)}, "
            f"veo3={next_veo}/{len(veo_all)}, wan26={next_wan}/{len(wan_all)}"
        )

        changed = sky_m_added > 0 or sky_s_added > 0 or veo_added > 0 or wan_added > 0
        if args.commit or args.push:
            save_state(state)
            run_git("add", "index.html", ".upload_state.json", "rotate_publish.py")
            for key in DATASET_KEYS:
                run_git("add", DATASETS[key]["asset_dir"].relative_to(REPO_ROOT).as_posix())
            if changed:
                msg = (
                    f"Upload batch: sky_multi+{sky_m_added}, sky_single+{sky_s_added}, "
                    f"veo3+{veo_added}, wan26+{wan_added}"
                )
                run_git("commit", "-m", msg)
                print("Committed.")
            else:
                print("No new files to commit.")
        else:
            save_state(state)

        if args.push and changed:
            git_push_with_retry(args.max_retries, args.retry_wait)
            print("Pushed.")

        done = (
            next_sky_m >= len(sky_m_all)
            and next_sky_s >= len(sky_s_all)
            and next_veo >= len(veo_all)
            and next_wan >= len(wan_all)
        )
        if not args.until_done or done:
            if done:
                print("All videos have been uploaded.")
            break


HTML_HEAD = """<!DOCTYPE html>
<html lang="zh-Hans">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Video Gallery</title>
  <style>
    :root { --page:#f6f7f9; --surface:#fff; --text:#1a1d24; --muted:#5c6577; --border:#e2e6ee; --radius:12px; --shadow:0 4px 24px rgba(15,23,42,.06); }
    * { box-sizing:border-box; } body { margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--page); color:var(--text); line-height:1.55; font-size:15px; }
    .shell { max-width:1600px; margin:0 auto; padding:28px 20px 64px; }
    .source-switch { margin-bottom:18px; display:flex; align-items:center; gap:10px; background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:12px 14px; box-shadow:var(--shadow); }
    .source-switch label { color:var(--muted); font-weight:600; } .source-switch select { border:1px solid var(--border); border-radius:8px; padding:6px 10px; background:#fff; font-size:14px; }
    .dataset { display:none; } .dataset.is-active { display:block; }
    .toc { display:flex; flex-wrap:wrap; gap:10px; margin-bottom:24px; padding:16px; background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); box-shadow:var(--shadow); }
    .toc-link { display:inline-flex; align-items:center; gap:6px; padding:8px 14px; border-radius:999px; text-decoration:none; color:var(--text); font-weight:600; font-size:.9rem; border:1px solid var(--border); background:#fafbfc; }
    .toc-link .count { color:var(--muted); font-weight:500; font-size:.85rem; }
    .group { margin-bottom:40px; padding:24px; border-radius:16px; background:var(--group-bg); border:1px solid color-mix(in srgb,var(--accent) 22%,var(--border)); }
    .group-header { display:flex; align-items:baseline; justify-content:space-between; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
    .group-header h2 { margin:0; font-size:1.25rem; font-weight:700; color:var(--accent); } .group-meta { color:var(--muted); font-size:.9rem; }
    .grid { display:grid; grid-template-columns:repeat(4,1fr); gap:18px; } @media (max-width:1200px){.grid{grid-template-columns:repeat(2,1fr)}} @media (max-width:560px){.grid{grid-template-columns:1fr}}
    .card { background:var(--surface); border-radius:var(--radius); overflow:hidden; border:1px solid var(--border); box-shadow:var(--shadow); display:flex; flex-direction:column; }
    .video-wrap { aspect-ratio:16/9; background:#0f1218; } .video-wrap video { width:100%; height:100%; object-fit:contain; display:block; }
    .meta { padding:12px 14px 14px; display:flex; flex-direction:column; gap:8px; } .badge { align-self:flex-start; font-size:.75rem; font-weight:700; padding:4px 10px; border-radius:6px; background:color-mix(in srgb,var(--accent) 15%,#fff); color:var(--accent); }
    .prompt { margin:0; font-size:.88rem; color:var(--text); line-height:1.5; }
  </style>
</head>
<body>
  <div class="shell">
"""

HTML_TAIL = """
  </div>
  <script>
    (function () {
      const io = new IntersectionObserver(
        (entries) => {
          for (const e of entries) {
            if (!e.isIntersecting) continue;
            const v = e.target;
            const url = v.getAttribute("data-src");
            if (url && !v.getAttribute("src")) {
              v.src = url;
              v.removeAttribute("data-src");
              io.unobserve(v);
            }
          }
        },
        { root: null, rootMargin: "320px", threshold: 0.01 }
      );
      function observeActiveDatasetVideos() {
        const active = document.querySelector(".dataset.is-active");
        if (!active) return;
        active.querySelectorAll("video[data-src]").forEach((v) => io.observe(v));
      }
      const select = document.getElementById("sourceSelect");
      const datasets = document.querySelectorAll(".dataset");
      function switchDataset(key) {
        datasets.forEach((el) => el.classList.toggle("is-active", el.dataset.dataset === key));
        location.hash = "";
        window.scrollTo({ top: 0, behavior: "smooth" });
        requestAnimationFrame(() => observeActiveDatasetVideos());
      }
      select.addEventListener("change", (e) => switchDataset(e.target.value));
      observeActiveDatasetVideos();
    })();
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
