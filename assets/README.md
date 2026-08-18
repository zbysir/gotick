# gotick logo assets

- `logo.svg` — the canonical color mark. Use it anywhere it renders above ~24px.
- `logo-favicon.svg` — same mark with the pivot optically enlarged so it survives
  at favicon sizes. `logo-16.png` and `logo-32.png` are generated from this one;
  `logo-256.png` and `logo-512.png` come from `logo.svg`.
- `logo-mono.svg` — inherits `currentColor`; the pivot goes with it, so this one
  reads as a plain `t`.
- `logo-wordmark.svg` / `logo-wordmark-dark.svg` — the readme/header lockups for
  light and for `#0d1117` backgrounds.

Colors: Go cyan `#00ADD8`, pivot coral `#F04E3E`. Stroke width is 6 on a 64 grid,
and the display pivot's radius is 3 so it sits flush with the stroke — don't
enlarge it in `logo.svg`, that is what `logo-favicon.svg` is for.

The lowercase `t` doubles as a pair of clock hands: a short hour hand above the
crossbar, a long minute hand along it, crossing at the pivot.

Regenerate the PNGs with `rsvg-convert -w <size> -h <size> <svg> -o <png>`.
