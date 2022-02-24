import logging
from datetime import datetime
import math
from jsonschema import validate, ValidationError, SchemaError
from lxml import etree, objectify

LOGGER = logging.getLogger()


def determain_dt_format(input_str):
    # ensure this is a T in the input_str
    if "T" not in input_str:
        raise Exception("Must have a T in the datetime.")
    # split the datetime
    split_dt = input_str.split("T")
    # grab the date
    date = split_dt[0]
    # check if it has a doy
    has_doy = False
    if len(date.split("-")) < 3:
        has_doy = True

    # grab the time
    time = split_dt[1]
    # check if there are seconds
    has_seconds = False
    if len(time.split(":")) > 2:
        has_seconds = True

    # check if there are milliseconds
    time_split = time.split(".")

    has_ms = False
    has_z = False
    if len(time_split) > 1:
        has_ms = True
    if "Z" in time:
        has_z = True

    # build the dt formating string
    dt_format = "%Y"
    if has_doy:
        dt_format += "-%jT"
    else:
        dt_format += "-%m-%dT"

    if has_seconds:
        dt_format += "%H:%M:%S"
    else:
        dt_format += "%H:%M"

    if has_ms:
        dt_format += ".%f"

    if has_z:
        dt_format += "Z"

    return dt_format


def from_iso_to_dt(input_str):
    dt_format = determain_dt_format(input_str)
    return datetime.strptime(input_str, dt_format)


def from_dt_to_iso(input_dt, custom_format="%Y-%m-%dT%H:%M:%S.%fZ"):
    return input_dt.strftime(custom_format)


# def from_iso_to_dt(input_str, format="%Y-%m-%dT%H:%M:%S.%fZ"):
#     return datetime.strptime(input_str, format)


def set_transfer_status(doc):
    if "daac_delivery_status" in doc:
        if doc["daac_delivery_status"] == "SUCCESS":
            doc["transfer_status"] = "cnm_r_success"
        else:
            doc["transfer_status"] = "cnm_r_failure"
    else:
        if "daac_CNM_S_status" in doc:
            if doc["daac_CNM_S_status"] == "SUCCESS":
                doc["transfer_status"] = "cnm_s_success"
            else:
                doc["transfer_status"] = "cnm_s_failure"
        else:
            doc["transfer_status"] = "unknown"
    return doc


def split_extra_except_t(dt):
    split_dt = dt.split("T")
    half1 = "".join(split_dt[0].split("-"))
    half2 = "".join(split_dt[1].split(":")).split(".")[0]
    return "T".join([half1, half2])


def from_td_to_str(input_td):
    days = input_td.days
    seconds = input_td.seconds
    hours = 0
    minutes = 0

    while seconds >= 3600:
        hours += 1
        seconds -= 3600

    while seconds >= 60:
        minutes += 1
        seconds -= 60

    sign = days < 0

    return "{}{}T{}:{}:{}".format(
        "-" if sign else "",
        "{:03d}".format(abs(days)).rjust(3),
        hours if hours >= 10 else "0" + str(hours),
        minutes if minutes >= 10 else "0" + str(minutes),
        seconds if seconds >= 10 else "0" + str(seconds),
    )


def add_value_to_path(root, path, value):
    path = objectify.ObjectPath(path)
    path.setattr(root, objectify.DataElement(value, nsmap="", _pytype=""))


def create_xml_from_dict(parent, name, dic):
    xml_obj = objectify.SubElement(parent, name)
    for key in dic:
        path = name + "." + key
        if isinstance(dic[key], dict):
            add_value_to_path(
                xml_obj, path, create_xml_from_dict(xml_obj, key, dic[key])
            )
        elif isinstance(dic[key], list):
            add_value_to_path(
                xml_obj, path, create_xml_from_list(xml_obj, key, dic[key])
            )
        else:
            add_value_to_path(xml_obj, path, dic[key])

    return parent


def create_xml_from_list(parent, name, entry_list):
    for entry in entry_list:
        parent = create_xml_from_dict(parent, name, entry)
    return parent


def convert_to_xml_str(root_name, data):
    root = etree.Element(root_name)
    for key in data:
        if isinstance(data[key], dict):
            root = create_xml_from_dict(root, key, data[key])
        elif isinstance(data[key], list):
            root = create_xml_from_list(root, key, data[key])

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def magnitude(input_val):
    """
    ref: https://stackoverflow.com/a/52335468
    validate the input 0 value
    :param input_val: int - positive integer
    :return:
    """
    if input_val == 0:
        return 0
    return int(math.floor(math.log10(abs(input_val))))


def get_orbit_range_list(start_orbit, end_orbit):
    """
    For a given orbit range, returning a list of tuples with the same magnitude.

    assumption: the input values are not None (pls validate before calling this)

    example: input (13, 12345) => output [(13, 99), (100, 999), (1000, 9999), (10000, 12345)]

    :param start_orbit: int - positive integer
    :param end_orbit: int - positive integer
    :return:
    """
    if (
        end_orbit < start_orbit
    ):  # validation. if end is smaller than start, no need to have any list
        return [(start_orbit, end_orbit)]
    start_mag = magnitude(start_orbit)
    end_mag = magnitude(end_orbit)
    if start_mag == end_mag:  # magnitudes are the same. only need 1 list
        return [(start_orbit, end_orbit)]
    range_list = []
    for i in range(start_mag, end_mag + 1):
        if i == start_mag:  # the very start
            range_list.append((start_orbit, (10 ** (i + 1)) - 1))
            pass
        elif i == end_mag:  # the very end
            range_list.append((10 ** i, end_orbit))
            pass
        else:
            range_list.append((10 ** i, (10 ** (i + 1)) - 1))
            pass
        pass
    return range_list


class ElasticsearchResultDictWrapper:
    def __init__(self, result_dict):
        self._my_dict = result_dict
        pass

    @staticmethod
    def __get_dict_val(dict_obj, key_list, default):
        """
        Helper function: might need retuning to work
        for a complicated dictionary. it walks down to get the value of a key from the list
        whoever calls it should validate for None returns.

        :param dict_obj: dictionary object.
        :param key_list: a list of keys to be walked
        :param default: Default value / datatype to return if key is missing. defaulted to None
        :return: None or the value
        """
        sub_obj = dict_obj
        for i in key_list:
            if isinstance(sub_obj, dict):
                if i not in sub_obj:
                    return default
                sub_obj = sub_obj[i]
                pass
            elif isinstance(sub_obj, list):
                temp_obj = []
                for each in sub_obj:
                    if isinstance(each, dict):
                        if i in each:
                            temp_obj.append(each[i])
                        pass
                    pass
                if len(temp_obj) < 1:
                    return default
                elif len(temp_obj) == 1:
                    sub_obj = temp_obj[0]
                    pass
                else:
                    sub_obj = temp_obj
                pass
            else:
                return default
            pass
        return sub_obj

    def get_val(self, dotted_keys, default=None):
        """
        Helper function to retrieve the values of a key

            elastic-search style keys separated by a '.'

        :param dotted_keys: str - '.' separated keys
        :param default: Default value / datatype to return if key is missing. defaulted to None
        :return:
        """
        if dotted_keys is None or dotted_keys == "":
            return default
        key_list = dotted_keys.split(".")
        return self.__get_dict_val(self._my_dict, key_list, default)

    pass


def is_valid_json(instance, schema):
    try:
        validate(instance=instance, schema=schema)
    except ValidationError:
        # LOGGER.error('{} is invalid json'.format(instance))
        return False
    except SchemaError:
        LOGGER.fatal("invalid schema: {}".format(schema))
        return False
    return True


def chunk_list(input_list, chunked_size):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(input_list), chunked_size):
        yield input_list[i : i + chunked_size]
        pass
    return
