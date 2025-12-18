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


@app.cell
def _():
    mo.md(r"""
    ### Funksjoner
    """)
    return


app._unparsable_cell(
    r"""
    #### Skriv om denne selv, i en logik du skjønner og bruk som trenign. Ikke bare kopier.
    # Ting å tenke på: Size limit? Batch download. Rate limit, etc.


    def get_dtm_1m_per_feature(
        polygon_path: str, output_folder: str, buffer_m: float = 20.0
    ) -> list[str]:
        \"\"\"
        Download DTM 1m from Geonorge WCS for each polygon feature separately.

        Connects to the Norwegian Mapping Authority's WCS service and downloads
        1m resolution DTM (Digital Terrain Model) for the bounding box of each
        polygon feature in the input file.

        Parameters:
            polygon_path: Path to vector file containing polygon(s) (gpkg, shp, etc.)
            output_folder: Directory for output GeoTIFF files
            buffer_m: Buffer around each bounding box in meters. Use larger values
                      for terrain analysis (500-1000m for hydrology, 50-100m for slope)

        Returns:
            List of paths to saved GeoTIFF files

        Example:
            >>> paths = get_dtm_1m_per_feature(
            ...     polygon_path=\"mires/selected_mires.gpkg\",
            ...     output_folder=\"DTM/DTM1\",
            ...     buffer_m=500
            ... )
            >>> for p in paths:
            ...     print(gu.Raster(p).info())
        \"\"\"

        polygons = gpd.read_file(polygon_path).to_crs(\"EPSG:25833\") #leser inn polygonene og reprojecter til riktig CRS    
        individual_bboxes = polygons.bounds #bruker geopandas til å finne boundingboxes   for hvert polygon 

    
        wcs_url = \"https://wcs.geonorge.no/skwms1/wcs.hoyde-dtm-nhm-25833\"
        coverage_id = \"nhm_dtm_topo_25833\"
        wcs = WebCoverageService(wcs_url, version=\"2.0.1\")

        for idx, row in individual_bboxes.iterrows():
            response = wcs.getCoverage(
                identifier=[coverage_id],
                subsets=[
                    (\"x\", row[\"minx\"], row[\"maxx\"]),
                    (\"y\", row[\"miny\"], row[\"maxy\"]),
                ],
                format=\"image/tiff\",
            )
        
        time.sleep(1)  # Wait 1 second between requests





      Path(output_folder).mkdir(parents=True, exist_ok=True)
        output_paths = []

      output_path = Path(output_folder) / f\"dtm_1m_{idx}.tif\"
            output_path.write_bytes(response.read())
            output_paths.append(str(output_path))
            print(f\"✓ Downloaded {idx}: {output_path.name}\")


    return output_paths
    """,
    name="_"
)


@app.function
def clip_raster_with_vector(
    input_raster_path: str, clipping_vector_path: str, output_raster_path: str
) -> str:
    """
    Clip a raster to a vector polygon boundary and save the result.

    Reprojects the vector to match the raster CRS, crops to the bounding box,
    then masks values outside the polygon to NoData.

    Parameters:
        input_raster_path: Path to input raster file (tif, vrt, etc.)
        clipping_vector_path: Path to vector file with clipping polygon(s)
        output_raster_path: Path for output GeoTIFF

    Returns:
        Path to saved clipped raster

    Example:
        >>> clip_raster_with_vector(
        ...     input_raster_path="DTM/DTM1/dtm_1m_0.tif",
        ...     clipping_vector_path="mires/mire_0.gpkg",
        ...     output_raster_path="DTM/DTM1/dtm_1m_0_clipped.tif"
        ... )
    """
    rast = gu.Raster(input_raster_path)
    vect = gu.Vector(clipping_vector_path).reproject(rast)

    rast_clipped = rast.crop(vect).mask(vect)
    rast_clipped.save(output_raster_path)

    return output_raster_path


@app.function
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


@app.cell(column=1)
def _():
    return


@app.cell
def _():
    mo.md(r"""
    Må rydde opp i alle input/oputt filer - bør defineres bedre
    """)
    return


app._unparsable_cell(
    r"""
    #input for å hente dtm1m
    input_polygons_path = vektor/###
    """,
    name="_"
)


app._unparsable_cell(
    r"""
    #input/out for klipping (om det trengs, dvs. at du ikke henter er lik det du vil klippe. da trenger du ikke denne)

    clipping_vector = 

    input_raster = \"DTM/DTM1/dtm_1m.tif\"

    output_raster = \"DTM/DTM1/dtm_1m_clipped.tif\"
    """,
    name="_"
)


@app.cell
def _():
    # INput/output for VRT bygging

    # Lager VRT av DTM filer (landsdekkende)

    dtm_folder = "DTM/DTM10/DTM10_UTM33_20251128"

    output_vrt_dtm = "DTM/vrt/dtm10.vrt"

    # lister all .tiff filer
    dtm_files = glob(os.path.join(dtm_folder, "*.tif"))

    # kaller vrt bygger funksjonen
    dtm_vrt = build_vrt(dtm_files, output_vrt_dtm)
    return (output_vrt_dtm,)


@app.cell
def _():
    # Initialize the tool
    wbt = whitebox.WhiteboxTools()

    # Optional: Set verbose to False to keep the notebook clean
    # (WBT prints a lot of text by default)
    wbt.set_verbose_mode(False)

    # Check if it works
    print(f"WhiteboxTools Version: {wbt.version()}")
    return


@app.cell(column=2)
def _():
    return


@app.cell
def _(output_vrt_dtm):
    dtm_10_raster = gu.Raster(output_vrt_dtm)
    dtm_10_raster.info()
    return


if __name__ == "__main__":
    app.run()
