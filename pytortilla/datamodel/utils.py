from typing import Literal, Tuple
from importlib.resources import files
from pyproj import CRS, Transformer

import pandas as pd

GEEPRODUCTS = {
    "ele": "projects/sat-io/open-datasets/GLO-30",
    "cisi": "projects/sat-io/open-datasets/CISI/global_CISI",
    "gdp": "projects/sat-io/open-datasets/GRIDDED_HDI_GDP/GDP_PPP_1990_2015_5arcmin_v2",
    "hdi": "projects/sat-io/open-datasets/GRIDDED_HDI_GDP/HDI_1990_2015_v2",
    "gmi": "projects/sat-io/open-datasets/GHM/ghm_v15_2017_300_60land",
    "pop": "projects/sat-io/open-datasets/hrsl/hrslpop",
    "admin0": "projects/ee-csaybar-real/assets/admin0",
    "admin1": "projects/ee-csaybar-real/assets/admin1",
    "admin2": "projects/ee-csaybar-real/assets/admin2"
}


def fetch_gee_metadata(image, points, reducer, scale):
    """Fetch metadata from an Earth Engine image."""
    data = image.reduceRegions(collection=points, reducer=reducer, scale=scale).getInfo()
    return pd.DataFrame([d["properties"] for d in data["features"]]).fillna(0)


def load_admin_codes():
    """Load administrative code mapping data."""
    base_path = files("pytortilla").joinpath("datamodel/data")
    return {
        level: pd.read_parquet(base_path / f"rai_admin{level}.parquet")
        for level in ["0", "1", "2"]
    }

def map_admin_codes(admin_dfs, metadata_df):
    """Map administrative codes to descriptive names."""
    for level, admin_df in admin_dfs.items():
        metadata_df = (
            metadata_df.join(admin_df, on=f"admin_code{level}", how="left", rsuffix="_rai")
            .drop(columns=[f"admin_code{level}", f"admin_code{level}_rai"])
        )
    metadata_df.columns = ["admin0", "admin1", "admin2"]
    return metadata_df


def raster_centroid(
    crs: str,
    geotransform: Tuple[float, float, float, float, float, float],
    raster_shape: Tuple[int, int],
) -> str:
    """
    Calculate the centroid of a raster in EPSG:4326 and return a WKT string.

    Args:
        crs (str): The raster's Coordinate Reference System (e.g., "EPSG:32633").
        geotransform (Tuple[float, float, float, float, float, float]): The
            geotransform of the raster following the GDAL convention:
            (
                top left x,
                x resolution,
                x rotation,
                top left y,
                y rotation,
                y resolution
            )
        raster_shape (Tuple[int, int]): The shape of the raster as (rows, columns).

    Returns:
        str: The centroid coordinates in EPSG:4326 as a WKT string.
    """
    # Extract geotransform parameters
    origin_x, pixel_width, _, origin_y, _, pixel_height = geotransform
    rows, cols = raster_shape

    # Compute raster centroid in the raster CRS
    centroid_x = origin_x + (cols / 2) * pixel_width
    centroid_y = origin_y + (rows / 2) * pixel_height

    # Transform centroid to EPSG:4326 using pyproj
    transformer = Transformer.from_crs(
        CRS.from_string(crs), CRS.from_epsg(4326), always_xy=True
    )
    lon, lat = transformer.transform(centroid_x, centroid_y)

    # precision of 6 decimal places
    lon = round(lon, 6)
    lat = round(lat, 6)

    return f"POINT ({lon} {lat})"


GDAL_FILES = Literal[
    "VRT",
    "DERIVED",
    "GTiff",
    "COG",
    "NITF",
    "RPFTOC",
    "ECRGTOC",
    "HFA",
    "SAR_CEOS",
    "CEOS",
    "JAXAPALSAR",
    "GFF",
    "ELAS",
    "ESRIC",
    "AIG",
    "AAIGrid",
    "GRASSASCIIGrid",
    "ISG",
    "SDTS",
    "DTED",
    "PNG",
    "JPEG",
    "MEM",
    "JDEM",
    "GIF",
    "BIGGIF",
    "ESAT",
    "FITS",
    "BSB",
    "XPM",
    "BMP",
    "DIMAP",
    "AirSAR",
    "RS2",
    "SAFE",
    "PCIDSK",
    "PCRaster",
    "ILWIS",
    "SGI",
    "SRTMHGT",
    "Leveller",
    "Terragen",
    "netCDF",
    "HDF4",
    "HDF4Image",
    "ISIS3",
    "ISIS2",
    "PDS",
    "PDS4",
    "VICAR",
    "TIL",
    "ERS",
    "JP2OpenJPEG",
    "L1B",
    "FIT",
    "GRIB",
    "RMF",
    "WCS",
    "WMS",
    "MSGN",
    "RST",
    "GSAG",
    "GSBG",
    "GS7BG",
    "COSAR",
    "TSX",
    "COASP",
    "R",
    "MAP",
    "KMLSUPEROVERLAY",
    "WEBP",
    "PDF",
    "Rasterlite",
    "MBTiles",
    "PLMOSAIC",
    "CALS",
    "WMTS",
    "SENTINEL2",
    "MRF",
    "PNM",
    "DOQ1",
    "DOQ2",
    "PAux",
    "MFF",
    "MFF2",
    "GSC",
    "FAST",
    "BT",
    "LAN",
    "CPG",
    "NDF",
    "EIR",
    "DIPEx",
    "LCP",
    "GTX",
    "LOSLAS",
    "NTv2",
    "CTable2",
    "ACE2",
    "SNODAS",
    "KRO",
    "ROI_PAC",
    "RRASTER",
    "BYN",
    "NOAA_B",
    "NSIDCbin",
    "ARG",
    "RIK",
    "USGSDEM",
    "GXF",
    "BAG",
    "S102",
    "HDF5",
    "HDF5Image",
    "NWT_GRD",
    "NWT_GRC",
    "ADRG",
    "SRP",
    "BLX",
    "PostGISRaster",
    "SAGA",
    "XYZ",
    "HF2",
    "OZI",
    "CTG",
    "ZMap",
    "NGSGEOID",
    "IRIS",
    "PRF",
    "EEDAI",
    "DAAS",
    "SIGDEM",
    "HEIF",
    "TGA",
    "OGCAPI",
    "STACTA",
    "STACIT",
    "GPKG",
    "OpenFileGDB",
    "CAD",
    "PLSCENES",
    "NGW",
    "GenBin",
    "ENVI",
    "EHdr",
    "ISCE",
    "Zarr",
    "HTTP",
    "BYTES",
    "TORTILLA",
]
