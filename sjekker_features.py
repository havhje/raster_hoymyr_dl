import marimo

__generated_with = "0.19.1"
app = marimo.App(width="columns")

with app.setup:
    # Initialization code that runs before all other cellsimport marimo as mo
    from osgeo import gdal
    from glob import glob
    import xarray
    import os
    import marimo as mo
    import rasterio
    import whitebox
    import geoutils as gu
    from owslib.wcs import WebCoverageService
    from pathlib import Path
    import geopandas as gpd
    import time
    import polars as pl
    from pyogrio import read_arrow
    import tempfile


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Leser inn grunnlagsdata
    """)
    return


@app.cell
def _():
    # MI-typer bare Nordland
    mi_typer_path = Path("vektor_inndata/mi_typer_nordland.json")

    # All myr i Nordland
    grunnkart_nordland_path = Path("vektor_inndata/grunnkart_nordland.geojson")
    return grunnkart_nordland_path, mi_typer_path


@app.cell
def _(mi_typer_path):
    # bruker pyogrio engine for raskere lesing
    mi_typer_gdf = gpd.read_file(mi_typer_path, layer="Naturtype_nin_omr", engine="pyogrio")

    mi_typer_våtmark = mi_typer_gdf[mi_typer_gdf["hovedøkosystem"] == "våtmark"][["geometry"]]

    mi_typer_våtmark.to_parquet("vektor_inndata/mi_typer_våtmark_nordland.parquet")
    return (mi_typer_gdf,)


@app.cell
def _(mi_typer_gdf):
    høymyr_liste = ["Atlantisk høymyr", "Kanthøymyr", "Platåhøymyr", "Eksentrisk høymyr", "Konsentrisk høymyr"]

    høymyr_nordland = mi_typer_gdf[mi_typer_gdf["naturtype"].isin(høymyr_liste)][["geometry"]]

    høymyr_nordland.to_parquet("vektor_inndata/høymyr_nordland.parquet")
    return


@app.cell
def _(grunnkart_nordland_path):
    # bruker pyogrio engine for raskere lesing
    grunnkart_nordland_gdf = gpd.read_file(grunnkart_nordland_path, engine="pyogrio")

    myr_nordland = grunnkart_nordland_gdf[grunnkart_nordland_gdf["ecotype"] == 7][["geometry"]]

    myr_nordland.to_parquet("vektor_inndata/myr_nordland.parquet")
    return


@app.cell
def _():
    # Leser inn parquet filene

    mi_typer_våtmark_filtrert_path = Path("vektor_inndata/mi_typer_våtmark_nordland.parquet")
    mi_typer_våtmark_filtrert = gpd.read_parquet(mi_typer_våtmark_filtrert_path).to_crs("EPSG:25833")

    myr_nordland_filtrert_path = Path("vektor_inndata/myr_nordland.parquet")
    myr_nordland_filtrert = gpd.read_parquet(myr_nordland_filtrert_path).to_crs("EPSG:25833")
    return (mi_typer_våtmark_filtrert,)


@app.cell
def _():
    return


@app.cell(column=1, hide_code=True)
def _():
    mo.md(r"""
    ## TO DO:

    🔴 Critical Fixes
    - 🚀 Parallel Processing (for 17k polygons)


    🟢 Error Handling
    -Log failed polygon indices to a list for retry
    - Add timeout parameter to WCS requests
    """)
    return


@app.cell
def _(mi_typer_våtmark_filtrert):
    polygon = mi_typer_våtmark_filtrert.head(5)  # bruker bare de fem første polygonene for testing
    output_folder = Path("raster_output/raster_mi_myr")

    # eller når du skal gjøre prediksjoner

    # polygon = myr_nordland_filtrert
    # output_folder = Path("raster_output/raster_myr_nordland")
    return output_folder, polygon


@app.cell
def _(polygon):
    # bruker geopandas til å finne boundingboxes for hvert polygon
    individual_bboxes = polygon.bounds
    return (individual_bboxes,)


@app.cell
def _():
    # Leser data fra geonorge med WCS (bruker 1.0.0 siden 2.0.1 er feilkonfigurert)
    wcs_url = "https://wcs.geonorge.no/skwms1/wcs.hoyde-dtm-nhm-25833"
    coverage_id = "nhm_dtm_topo_25833"
    wcs = WebCoverageService(wcs_url, version="1.0.0")
    return coverage_id, wcs


@app.cell
def _(coverage_id, individual_bboxes, output_folder, polygon, wcs):
    # leser hvert polygon og laster ned dtm 1m for hver bbox. Skriver til output mappe.
    # idx = indeksen til hver enkelt rad, bruker denne til navgivning av filer
    # row = selve raden med bbox info (minx, miny, maxx, maxy).
    # Må bruke .itertuple() for å iterere/loope over hver rad, hvis du ikke bruker denne så looper du over bare selve kolonne(navnene)


    for row in individual_bboxes.itertuples():
        # Lager bbox tuple for WCS 1.0.0 (minx, miny, maxx, maxy)
        index = row.Index  # Henter indeksen til raden
        bbox = (
            float(row.minx),
            float(row.miny),
            float(row.maxx),
            float(row.maxy),
        )

        # Beregner pixelstørrelse for ~1m oppløsning
        width = max(1, int(row.maxx) - (row.minx))
        height = max(1, int(row.maxy) - (row.miny))

        # WCS 1.0.0 bruker bbox og crs i stedet for subsets
        response_geonorge = wcs.getCoverage(
            identifier=coverage_id,  # String, ikke liste i WCS 1.0.0
            bbox=bbox,
            crs="EPSG:25833",
            format="GeoTIFF",
            width=width,
            height=height,
        )

        nedlastet_data = response_geonorge.read()  # Laster ned data

        # Lager en temp fil som du skriver responsdataene til fra geonorge
        temp = Path(tempfile.gettempdir()) / f"temp_{index}.tif"
        temp.write_bytes(nedlastet_data)

        # Bruker try/finally for å sikre opprydding selv ved feil
        raster = None
        vector = None
        mask = None
        try:
            raster = gu.Raster(temp, load_data=True)

            # Maskerer så data utenfor polygonet, men innenfor bb som NoData
            # .loc er pandas kode for å hente en rad basert på index [[]] gir en df
            vector = gu.Vector(polygon.loc[[index]])  # henter samme polygonet som rasteren er laget fra

            # Lager en maske (raster) basert på vektor polygonet hvor innsiden polygon = TRUE og utsiden = False
            mask = vector.create_mask(ref=raster)  # setter ref for å få lik CRS, extent, osv.

            # Bruker masken til å sette verdier i rasteren.
            # Innvertere med ~ ettersom set_mask() gir True = NoData
            # ~mask = False inside, True outside
            # False pixels (inside) → remain visible
            # True pixels (outside) → become NoData
            raster.set_mask(~mask)

            output = output_folder / f"D_1m_{index}.tif"
            raster.save(output)

        finally:
            # Eksplisitt opprydding for å forhindre minnelekkasje
            # Sletter objektene i omvendt rekkefølge av opprettelse
            if mask is not None:
                del mask
            if vector is not None:
                del vector
            if raster is not None:
                # Lukk underliggende GDAL dataset hvis tilgjengelig
                if hasattr(raster, "_ds") and raster._ds is not None:
                    raster._ds = None
                del raster

        # Sletter midlertidig fil
        temp.unlink()

        time.sleep(1)
    return


@app.cell(column=2, hide_code=True)
def _():
    mo.md(r"""
    ### Gammelt
    """)
    return


@app.function(hide_code=True)
def build_vrt(input_files: list[str], output_path: str) -> gdal.Dataset:
    """
    Build a GDAL VRT (Virtual Raster) from multiple input files.

    Creates a lightweight virtual mosaic that references the source files
    without copying data. Useful for treating multiple tiles as one raster.

    Parameters:
        input_files: List of paths to input raster files (.tif)
        output_path: Path for output VRT file (must end with .vrt)

    Returns:
        GDAL Dataset object of the created VRT

    Example:
        >>> tif_files = glob("DTM/tiles/*.tif")
        >>> vrt = build_vrt(tif_files, "DTM/mosaic.vrt")
        >>> raster = gu.Raster("DTM/mosaic.vrt")
    """
    results = gdal.BuildVRT(output_path, input_files)

    return results


@app.cell(hide_code=True)
def _():
    # INput/output for VRT bygging

    # Lager VRT av DTM filer (landsdekkende)

    dtm_folder = "1m_DTM_raster"

    output_vrt_dtm = "dtm_1m.vrt"

    # lister all .tiff filer
    dtm_files = glob(os.path.join(dtm_folder, "*.tif"))

    # kaller vrt bygger funksjonen
    dtm_vrt = build_vrt(dtm_files, output_vrt_dtm)
    return (output_vrt_dtm,)


@app.cell
def _(output_vrt_dtm):
    dtm_1_raster = gu.Raster(output_vrt_dtm)
    dtm_1_raster.info()
    return


if __name__ == "__main__":
    app.run()
