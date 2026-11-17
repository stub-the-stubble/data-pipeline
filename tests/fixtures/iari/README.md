# Public IARI parser fixtures

These are excerpts of public source evidence, not complete source downloads.

- `summary-tables.txt`: observation/issue headings, state daily tables and district
  summary tables extracted from the 15 November 2026 bulletin, which covers
  15 September–15 November. Expected totals: Punjab 5,972; Haryana 242.
- `oct21-events.txt`: event tables from the 21 October bulletin, issued 22 October;
  70 Punjab and two Haryana rows. This tests observation date vs issue date.
- `wfs-excerpt.json`: one real 15 November Punjab event. Collection counters are
  reduced to one to make this a self-contained parser fixture, not a daily total.

Sources: https://creams.iari.res.in/geokrishi/geoserver/wfs and
https://creams.iari.res.in/Creams_website_bulletin/Fire_bulletin_2026/Fire_bulletin_2026r/62.RiceResidueFireBulletin_15Nov_2026_ICAR.pdf

Full PDFs and all daily WFS responses belong in an external frozen source bundle.
Unit fixtures do not substitute for replaying that bundle before publication.
