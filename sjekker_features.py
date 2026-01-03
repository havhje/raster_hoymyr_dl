import marimo

__generated_with = "0.18.4"
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
    mi_typer_gdf = gpd.read_file(
        mi_typer_path, layer="Naturtype_nin_omr", engine="pyogrio"
    )

    mi_typer_våtmark = mi_typer_gdf[mi_typer_gdf["hovedøkosystem"] == "våtmark"][
        ["geometry"]
    ]

    mi_typer_våtmark.to_parquet("vektor_inndata/mi_typer_våtmark_nordland.parquet")
    return


@app.cell
def _(grunnkart_nordland_path):
    # bruker pyogrio engine for raskere lesing
    grunnkart_nordland_gdf = gpd.read_file(
        grunnkart_nordland_path, engine="pyogrio"
    )

    myr_nordland = grunnkart_nordland_gdf[grunnkart_nordland_gdf["ecotype"] == 7][
        ["geometry"]
    ]

    myr_nordland.to_parquet("vektor_inndata/myr_nordland.parquet")
    return


@app.cell(column=1)
def _():
    # input for å hente dtm1m
    input_polygons_path = Path("vektor_inndata/polygon_andøya")
    return (input_polygons_path,)


@app.cell
def _(input_polygons_path):
    get_dtm_1m_per_polygon(input_polygons_path, "1m_DTM_raster")
    return


@app.cell
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


@app.cell
def _():
    mo.md(r"""
    # Parallel prosessering og du må fikse riktig navn på outputfilene i clip raster funksjonen i for loopen.
    """)
    return


@app.cell(column=2, hide_code=True)
def _():
    mo.md(r"""
    ### Funksjoner
    """)
    return


@app.function(hide_code=True)
def get_dtm_1m_per_polygon(polygon_path: str, output_folder: str):
    """
    Download 1m DTM tiles from Geonorge WCS for each polygon feature.

    Reads polygon geometries, extracts bounding boxes, and downloads
    corresponding DTM 1m data from Kartverket's WCS service. Each feature
    gets a separate GeoTIFF saved to the output folder.

    Parameters:
        polygon_path: Path to vector file containing polygon geometries
        output_folder: Directory path where downloaded DTM tiles will be saved

    Returns:
        None. Files are saved as D_1m_{index}.tif in output_folder.

    Example:
        >>> input_polygons = "vektor_inndata/polygon_andoya.shp"
        >>> get_dtm_1m_per_feature(input_polygons, "1m_DTM_raster")
        # Creates: 1m_DTM_raster/D_1m_0.tif, D_1m_1.tif, etc.

    Note:
        - Requires internet connection to Geonorge WCS service
        - Input polygons are reprojected to EPSG:25833 automatically
        - Includes 1 second delay between requests to avoid rate limiting
        - Uses WCS 1.0.0 since 2.0.1 is misconfigured on server side
    """

    # Leser data fra geonorge med WCS (bruker 1.0.0 siden 2.0.1 er feilkonfigurert)
    wcs_url = "https://wcs.geonorge.no/skwms1/wcs.hoyde-dtm-nhm-25833"
    coverage_id = "nhm_dtm_topo_25833"
    wcs = WebCoverageService(wcs_url, version="1.0.0")

    # Sørg for at output-mappen eksisterer
    Path(output_folder).mkdir(exist_ok=True)

    # Leser polygoner
    polygons = gpd.read_file(polygon_path).to_crs(
        "EPSG:25833"
    )  # leser inn polygonene og reprojecter til riktig CRS
    individual_bboxes = (
        polygons.bounds
    )  # bruker geopandas til å finne boundingboxes for hvert polygon

    # leser hvert polygon og laster ned dtm 1m for hver bbox. Skriver til output mappe.
    # idx = indeksen til hver enkelt rad, bruker denne til navgivning av filer
    # row = selve raden med bbox info (minx, miny, maxx, maxy).
    # Må bruke .iterrows() for å iterere/loope over hver rad, hvis du ikke bruker denne så looper du over bare selve kolonne(navnene)

    for index, row in individual_bboxes.iterrows():
        # Lager bbox tuple for WCS 1.0.0 (minx, miny, maxx, maxy)
        bbox = (
            float(row["minx"]),
            float(row["miny"]),
            float(row["maxx"]),
            float(row["maxy"]),
        )

        # Beregner pixelstørrelse for ~1m oppløsning
        width = max(1, int(row["maxx"] - row["minx"]))
        height = max(1, int(row["maxy"] - row["miny"]))

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

        output = (
            Path(output_folder) / f"D_1m_{index}.tif"
        )  # lager fila hvor dataene skrives til

        output.write_bytes(nedlastet_data)  # Skriver nedlastede data til fil

        time.sleep(1)


@app.cell
def _(index, input_clip_polygon_path):
    def clip_raster_with_polygon(
        input_clip_polygons_path,
        raster_folder_path,
        output_folder,
    ):
        for raster_geotiff in raster_folder_path.glob("*.tif"):
            vector = gu.Vector(input_clip_polygon_path)

            raster = gu.Raster(raster_geotiff)

            # Lager en maske (raster) hvor innsiden polygon = TRUE og utsiden = False
            mask = raster.create_mask(vector)

            # Bruker masken til å sette verdier i rasteren.
            # Innvertere med ~ ettersom set_mask() gir True = NoData
            # ~mask = False inside, True outside
            # False pixels (inside) → remain visible
            # True pixels (outside) → become NoData
            raster.set_mask(~mask)

            output = Path(output_folder) / f"D_1m_{index}.tif"

            raster.save(output)
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


if __name__ == "__main__":
    app.run()
