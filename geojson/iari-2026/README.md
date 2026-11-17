# IARI district display geography

These files are reference outlines, never inputs to event classification. IARI's
reported district determines each count. The legacy NRSC boundaries are retained.

Punjab has 23 districts, including Malerkotla. Its outlines and Haryana's parent
districts come from IARI's public WFS layer `geokrishi_v:district_boundary_ll_soi`:
https://creams.iari.res.in/geokrishi/geoserver/wfs

That layer still places Hansi in Hisar. Hansi is derived from LGD blocks 487
(Hansi-I), 488 (Hansi-II), and 491 (Narnaund). The district administration lists
exactly these three blocks: https://hansi.haryana.gov.in/subdivision-blocks/

The LGD source is the India Geodata `admin/blocks` release:
https://github.com/yashveeeeeeer/india-geodata/releases/tag/admin/blocks

`LGD_Blocks.parquet` SHA-256:
`db29c41da1bc30861d227b1a686cd25267ed8c70375587e2a9e760d7ff2542d7`.
The local input matched the published release asset digest. The three extracted
features are retained in `hansi-blocks-source.geojson`. This is a derived reference
boundary from older LGD geography and current block membership, not a newly
surveyed or certified Hansi district vector. Do not use it to identify land parcels.

The union is clipped to the IARI/SOI Hisar outline at the source seam (less than
0.00001% of the block union's area). Its complement becomes Hisar; their combined
footprint is unchanged. The builder rejects material discrepancies, invalid
polygons, duplicate districts and overlapping interiors. Full-resolution GeoJSON
is retained. Frontend SVG uses 40-metre display simplification and preserves holes.

Regenerate with the frozen publisher response and the checked-in block extract:

```sh
python tools/build_iari_maps.py --publisher-boundaries ../evidence/iari-district-boundaries.json --hansi-blocks geojson/iari-2026/hansi-blocks-source.geojson --webapp ../webapp
```

`provenance.json` records input and output hashes. The 15 September–15 November
2026 candidate has all six Hansi points inside the derived outline and all ten
Hisar points outside it. Across both states, 98 Punjab and two Haryana point
centres fall outside their reported district outlines, including seven Malerkotla
points. Counts retain publisher labels; outlines and observation centres are not
an exact spatial classification contract.
