# Code by Lachlan Marnoch, 2021

import urllib
from datetime import date
from typing import Union
from astropy.coordinates import SkyCoord

from craftutils import params as p
from craftutils import utils as u

fors2_filters_retrievable = ["I_BESS", "R_SPEC", "b_HIGH", "v_HIGH"]
sdss_filters = ["u", "g", "r", "i", "z"]


def retrieve_fors2_calib(fil: str = 'I_BESS', date_from: str = '2017-01-01', date_to: str = None):
    """
    Retrieves the full set of photometry parameters from the FORS2 quality control archive
    (http://archive.eso.org/bin/qc1_cgi?action=qc1_browse_table&table=fors2_photometry), from date_from to date_to.
    :param fil: The filter for which the data is to be retrieved. Must be "I_BESS", "R_SPEC", "b_HIGH" or "v_HIGH".
    :param date_from: The date from which to begin.
    :param date_to: The date on which to end. If None, defaults to current date.
    :return: The table of parameters, as a string.
    """
    if fil not in fors2_filters_retrievable:
        raise ValueError(f"{fil} not recognised; fil must be one of {fors2_filters_retrievable}")
    if date_to is None:
        date_to = str(date.today())
    # Construct the data expected by the FORS2 QC1 archive to send as a request.
    request = {
        "field_mjd_obs": "mjd_obs",
        "field_civil_date": "civil_date",
        "field_zeropoint": "zeropoint",
        "field_zeropoint_err": "zeropoint_err",
        "field_colour_term": "colour_term",
        "field_colour_term_err": "colour_term_err",
        "field_colour": "colour",
        "field_extinction": "extinction",
        "field_extinction_err": "extinction_err",
        "field_num_ext": "num_ext",
        "field_num_fields": "num_fields",
        "field_num_nights": "num_nights",
        "field_date_range": "date_range",
        "field_stable": "stable",
        "field_filter_name": "filter_name",
        "filter_filter_name": fil,
        "field_det_chip1_id": "det_chip1_id",
        "filter_det_chip1_id": "CCID20-14-5-3",
        "field_det_chip_num": "det_chip_num",
        "filter_det_chip_num": "1",
        "from": date_from,
        "to": date_to,
        "action": "qc1_browse_get",
        "table": "fors2_photometry",
        "output": "ascii",
    }
    request = urllib.parse.urlencode(request)
    request = bytes(request, 'utf-8')
    page = urllib.request.urlopen("http://archive.eso.org/qc1/qc1_cgi", request)
    return str(page.read().replace(b'!', b''), 'utf-8')


def save_fors2_calib(output: str, fil: str = 'I_BESS', date_from: str = '2017-01-01', date_to: str = None):
    """
    Retrieves the full set of photometry parameters from the FORS2 quality control archive
    (http://archive.eso.org/bin/qc1_cgi?action=qc1_browse_table&table=fors2_photometry), from date_from to date_to,
    formats them conveniently for numpy to read, and writes them to disk at the location given by output.
    :param output: The location on disk to which to write the file.
    :param fil: The filter for which the data is to be retrieved. Must be "I_BESS", "R_SPEC", "b_HIGH" or "v_HIGH".
    :param date_from: The date from which to begin.
    :param date_to: The date on which to end.
    :return: The table of parameters, as a string.
    """
    print(f"Updating ESO QC1 parameters for {fil} to {output}")
    string = retrieve_fors2_calib(fil=fil, date_from=date_from, date_to=date_to)
    i = j = string.find('\n') + 1
    while string[j] == '-':
        j += 1
    string = string[:i] + string[j + 1:]
    with open(output, "w") as file:
        file.write(string)
    return string


def update_fors2_calib():
    """
    Runs save_fors2_calib() for all four retrievable FORS2 filters
    :return:
    """
    for fil in fors2_filters_retrievable:
        path = p.config['photom_calib_dir'] + fil + '.txt'
        save_fors2_calib(output=path, fil=fil)
        if fil == 'R_SPEC':
            fil = 'R_SPECIAL'
        p.ingest_filter_properties(path=p.config['photom_calib_dir'] + fil + '.txt', instrument='FORS2')


def retrieve_sdss_photometry(ra: float, dec: float):
    try:
        from SciServer import Authentication, CasJobs
    except ImportError:
        print("It seems that SciScript/SciServer is not installed, or not accessible to this environment. "
              "\nIf you wish to automatically download SDSS data, please install "
              "\nSciScript (https://github.com/sciserver/SciScript-Python); "
              "\notherwise, retrieve the data manually from "
              "\nhttp://skyserver.sdss.org/dr16/en/tools/search/sql.aspx")
        return None

    print(f"Querying SDSS DR16 archive for field centring on RA={ra}, DEC={dec}")
    keys = p.keys()
    user = keys['sciserver_user']
    password = keys["sciserver_pwd"]
    Authentication.login(UserName=user, Password=password)
    # Construct an SQL query to send to SciServer
    query = "SELECT objid,ra,dec"
    for f in sdss_filters:
        query += f",psfMag_{f},psfMagErr_{f},fiberMag_{f},fiberMagErr_{f},fiber2Mag_{f},fiber2MagErr_{f},petroMag_{f},petroMagErr_{f}"
    query += " FROM PhotoObj "
    query += f"WHERE ra BETWEEN {ra - 0.1} AND {ra + 0.1} "
    query += f"AND dec BETWEEN {dec - 0.1} AND {dec + 0.1} "
    print("Retrieving photometry from SDSS DR16 via SciServer...")
    df = CasJobs.executeQuery(sql=query, context='DR16')
    return df


def save_sdss_photometry(ra: float, dec: float, output: str):
    df = retrieve_sdss_photometry(ra=ra, dec=dec)
    if df is not None:
        print("Saving SDSS photometry to" + output)
        df.to_csv(output)
        return df
    else:
        print("No data retrieved from SDSS.")
        return None


def update_std_sdss_photometry(ra: float, dec: float):
    data_dir = p.config['top_data_dir']
    path = f"{data_dir}/std_fields/RA{ra}_DEC{dec}/"
    u.mkdir_check(path)
    path += "SDSS/"
    u.mkdir_check(path)
    path += "SDSS.csv"

    return save_sdss_photometry(ra=ra, dec=dec, output=path)


def update_frb_sdss_photometry(frb: str):
    params = p.object_params_frb(frb)
    data_dir = params['data_dir']
    path = data_dir + "SDSS/"
    u.mkdir_check(path)
    path += "SDSS.csv"
    df = save_sdss_photometry(ra=params['burst_ra'], dec=params['burst_dec'], output=path)
    return df


