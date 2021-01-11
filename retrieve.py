import urllib
from datetime import date

from craftutils import params as p

retrievable_filters = ["I_BESS", "R_SPEC", "b_HIGH", "v_HIGH"]


def retrieve_fors2_calib(fil: str = 'I_BESS', date_from: str = '2017-01-01', date_to: str = None):
    """
    Retrieves the full set of photometry parameters from the FORS2 quality control archive
    (http://archive.eso.org/bin/qc1_cgi?action=qc1_browse_table&table=fors2_photometry), from date_from to date_to.
    :param fil: The filter for which the data is to be retrieved. Must be "I_BESS", "R_SPEC", "b_HIGH" or "v_HIGH".
    :param date_from: The date from which to begin.
    :param date_to: The date on which to end. If None, defaults to current date.
    :return: The table of parameters, as a string.
    """
    if fil not in retrievable_filters:
        raise ValueError(f"{fil} not recognised; fil must be one of {retrievable_filters}")
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
    print(request)
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
    for fil in retrievable_filters:
        path = p.config['photom_calib_dir'] + fil + '.txt'
        save_fors2_calib(output=path, fil=fil)
        if fil == 'R_SPEC':
            fil = 'R_SPECIAL'
        p.ingest_filter_properties(path=p.config['photom_calib_dir'] + fil + '.txt', instrument='FORS2')
