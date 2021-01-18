# Code by Lachlan Marnoch, 2021

import urllib
from datetime import date
from typing import Union
import requests
import os
import time

from craftutils import params as p
from craftutils import utils as u

keys = p.keys()

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
    """
    Retrieve SDSS photometry for a given field, in a 0.1 x 0.1 degree box centred on the passed coordinates
    coordinates. (Note - the width of the box is in RA degrees, not corrected for spherical distortion)
    :param ra: Right Ascension of the centre of the desired field, in degrees.
    :param dec: Declination of the centre of the desired field, in degrees.
    :return: Retrieved photometry table, as a pandas dataframe, if successful; if not, None.
    """
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
    """
    Retrieves and writes to disk the SDSS photometry for a given field, in a 0.1 x 0.1 degree box
    centred on the field coordinates. (Note - the width of the box is in RA degrees, not corrected for spherical
    distortion)
    :param ra: Right Ascension of the centre of the desired field, in degrees.
    :param dec: Declination of the centre of the desired field, in degrees.
    :param output: The location on disk to which to write the file.
    :return: Retrieved photometry table, as a pandas dataframe, if successful; if not, None.
    """
    df = retrieve_sdss_photometry(ra=ra, dec=dec)
    if df is not None:
        print("Saving SDSS photometry to" + output)
        df.to_csv(output)
        return df
    else:
        print("No data retrieved from SDSS.")
        return None


def update_std_sdss_photometry(ra: float, dec: float):
    """
    Retrieves and writes to disk the SDSS photometry for a standard-star calibration field, in a 0.1 x 0.1 degree box
    centred on the field coordinates. (Note - the width of the box is in RA degrees, not corrected for spherical
    distortion)
    :param ra: Right Ascension of the centre of the desired field, in degrees.
    :param dec: Declination of the centre of the desired field, in degrees.
    :return: Retrieved photometry table, as a pandas dataframe, if successful; if not, None.
    """
    data_dir = p.config['top_data_dir']
    path = f"{data_dir}/std_fields/RA{ra}_DEC{dec}/"
    u.mkdir_check(path)
    path += "SDSS/"
    u.mkdir_check(path)
    path += "SDSS.csv"

    return save_sdss_photometry(ra=ra, dec=dec, output=path)


def update_frb_sdss_photometry(frb: str):
    """
    Retrieve SDSS photometry for the field of an FRB (with a valid param file in param_dir), in a 0.1 x 0.1 degree box
    centred on the FRB coordinates, and
    (Note - the width of the box is in RA degrees, not corrected for spherical distortion)
    :param frb: FRB name, FRBXXXXXX. Must match title of param file.
    :return: Retrieved photometry table, as a pandas dataframe, if successful; if not, None.
    """
    params = p.object_params_frb(frb)
    data_dir = params['data_dir']
    path = data_dir + "SDSS/"
    u.mkdir_check(path)
    path += "SDSS.csv"
    df = save_sdss_photometry(ra=params['burst_ra'], dec=params['burst_dec'], output=path)
    return df


def retrieve_irsa_xml(ra: float, dec: float):
    """
    Retrieves the extinction parameters for a given sky position from the IRSA Dust Tool
    (https://irsa.ipac.caltech.edu/applications/DUST/)
    :param ra: Right Ascension of the desired field, in degrees.
    :param dec: Declination of the desired field, in degrees.
    :return: XML-formatted string.
    """
    url = f"https://irsa.ipac.caltech.edu/cgi-bin/DUST/nph-dust?locstr={ra}+{dec}+equ+j2000"
    print("\nRetrieving IRSA Dust Tool XML from", url)
    irsa_xml = urllib.request.urlopen(url)
    irsa_xml = irsa_xml.read()
    return str(irsa_xml, 'utf-8')


def retrieve_irsa_extinction(ra: float, dec: float):
    """
    Retrieves the extinction per bandpass table, and other relevant parameters, for a given sky position from the
    IRSA Dust Tool (https://irsa.ipac.caltech.edu/applications/DUST/).
    :param ra: Right Ascension of the desired field, in degrees.
    :param dec: Declination of the desired field, in degrees.
    :return: Tuple: dictionary of retrieved values, table-formatted string.
    """
    irsa_xml = retrieve_irsa_xml(ra=ra, dec=dec)

    to_retrieve = {"refPixelValueSandF": "E_B_V_SandF"}
    retrieved = {}
    for tag in to_retrieve:
        val_str = u.extract_xml_param(tag="refPixelValueSandF", xml_str=irsa_xml)
        print("val_str:", val_str)
        retrieved[to_retrieve[tag]], _ = u.unit_str_to_float(val_str)
    i = irsa_xml.find("extinction.tbl")
    j = i + 14
    substr = irsa_xml[i:i + 8]
    while substr != "https://":
        i -= 1
        substr = irsa_xml[i:i + 8]
    table_url = irsa_xml[i:j]
    print("Retrieving bandpass extinction table from", table_url)
    extinction = urllib.request.urlopen(table_url)
    ext_str = extinction.read()
    ext_str = str(ext_str, 'utf-8')
    return retrieved, ext_str


def save_irsa_extinction(ra: float, dec: float, output: str):
    """
    Retrieves the extinction per bandpass table for a given sky position from the IRSA Dust Tool
    (https://irsa.ipac.caltech.edu/applications/DUST/) and writes it to disk.
    :param ra: Right Ascension of the desired field, in degrees.
    :param dec: Declination of the desired field, in degrees.
    :param output: The location on disk to which to write the file.
    :return: Tuple: dictionary of retrieved values, table-formatted string.
    """
    values, ext_str = retrieve_irsa_extinction(ra=ra, dec=dec)
    ext_str = ext_str.replace("microns", "um")
    with open(output, "w") as file:
        file.write(ext_str)
    return values, ext_str


def update_frb_irsa_extinction(frb: str):
    """
    Retrieves the extinction per bandpass table, and other relevant parameters, for a given sky position from the
    IRSA Dust Tool (https://irsa.ipac.caltech.edu/applications/DUST/) and writes it to disk.
    :param frb: FRB name, FRBXXXXXX. Must match title of param file.
    :return: Tuple: dictionary of retrieved values, table-formatted string.
    """
    params = p.object_params_frb(frb)
    data_dir = params['data_dir']
    values, ext_str = save_irsa_extinction(ra=params['burst_ra'], dec=params['burst_dec'],
                                           output=data_dir + "galactic_extinction.txt")
    p.add_output_values_frb(obj=frb, params=values)
    return values, ext_str


# Dark Energy Survey database functions adapted code by T. Andrew Manning, from
# https://github.com/des-labs/desaccess-docs/blob/master/_static/DESaccess_API_example.ipynb

des_url = 'https://des.ncsa.illinois.edu'
des_api_url = des_url + "/desaccess/api"
des_files_url = des_url + "/files-desaccess"


def des_login():
    """
    Obtains an auth token using the username and password credentials for a given database.
    """
    # Login to obtain an auth token
    r = requests.post(
        f'{des_api_url}/login',
        data={
            'username': keys['des_user'],
            'password': keys['des_pwd'],
            'database': keys['database']
        }
    )
    # Store the JWT auth token
    keys['des_auth_token'] = r.json()['token']
    return keys['des_auth_token']


def des_check_auth_token():
    if 'des_auth_token' not in keys:
        raise KeyError("Use des_login() to log in before submitting requests.")


def des_submit_cutout_job(data: dict):
    """Submits a query job and returns the complete server response which includes the job ID."""

    des_check_auth_token()

    # Submit job
    r = requests.put(
        f'{des_api_url}/job/cutout',
        data=data,
        headers={'Authorization': f'Bearer {keys["auth_token"]}'}
    )
    response = des_check_success(r)

    return response


def des_check_success(response: requests.Response):
    response = response.json()

    if response['status'] == 'ok':
        job_id = response['jobid']
        print('Job "{}" submitted.'.format(job_id))
        # Refresh auth token
        keys['des_auth_token'] = response['new_token']
    else:
        job_id = None
        print('Error submitting job: '.format(response['message']))
    return response


def des_submit_query_job(query):
    """Submits a query job and returns the complete server response which includes the job ID."""

    des_check_auth_token()

    # Specify API request parameters
    data = {
        'username': keys['des_user'],
        'db': 'desdr',
        'filename': 'DES.csv',
        'query': query
    }

    # Submit job
    r = requests.put(
        f'{des_api_url}/job/query',
        data=data,
        headers={'Authorization': f'Bearer {keys["des_auth_token"]}'}
    )
    response = des_check_success(response=r)

    return response


def des_get_job_status(job_id):
    """Returns the current status of the job identified by the unique job_id."""

    des_check_auth_token()

    r = requests.post(
        f'{des_api_url}/job/status',
        data={
            'job-id': job_id
        },
        headers={'Authorization': f'Bearer {keys["des_auth_token"]}'}
    )
    response = r.json()
    # Refresh auth token
    keys['des_auth_token'] = response['new_token']
    # print(json.dumps(response, indent=2))
    return response


def des_job_status_poll(job_id):
    print(f'Polling status of job "{job_id}"...', end='')
    job_status = ''
    response = None
    while job_status != 'ok':
        # Fetch the current job status
        response = des_get_job_status(job_id)
        # Quit polling if there is an error getting a status update
        if response['status'] != 'ok':
            break
        job_status = response['jobs'][0]['job_status']
        if job_status == 'success' or job_status == 'failure':
            print(f'\nJob completed with status: {job_status}')
            break
        else:
            # Display another dot to indicate that polling is still active
            print('.', end='', sep='', flush=True)
        time.sleep(3)
    return response


def des_download_job_files(url, outdir):
    os.makedirs(outdir, exist_ok=True)
    r = requests.get(f'{url}/json')
    for item in r.json():
        if item['type'] == 'directory':
            suburl = f'{url}/{item["name"]}'
            subdir = f'{outdir}/{item["name"]}'
            des_download_job_files(suburl, subdir)
        elif item['type'] == 'file':
            data = requests.get('{}/{}'.format(url, item['name']))
            with open('{}/{}'.format(outdir, item['name']), "wb") as file:
                file.write(data.content)

    response = r.json()
    return response
