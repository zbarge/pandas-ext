import pandas as pd
import logging
import os

CODECS = ['utf8', 'iso-8859-1', 'ascii', 'utf-16', 'utf-32']


def path_incremented(p, overwrite=False):
    """
    Increments a file path so you don't overwrite
    an existing file (unless you choose to).
    :param p: (str)
        The file path to possibly increment
    :param overwrite: (str)
        True will just increment once and return the path.
        False will keep incrementing until the path is available
    :return:
        The original file path if it doesn't exist.
        The file path incremented by 1 until it doesn't exist.
    """
    dirname = os.path.dirname(p)
    error = 2
    while os.path.exists(p):
        name = os.path.basename(p)
        name, ext = os.path.splitext(name)
        try:
            val = ''.join(e for e in str(name)[-3:] if e.isdigit())
            count = int(val) + 1
            name = name.replace(val, str(count))
            if str(count) not in name:
                name = "{}{}".format(name, count)
        except ValueError:
            name = "{}{}".format(name, error)
            error += 1
        p = os.path.join(dirname, name + ext)
        if overwrite is True or error > 2500:
            break
    return p


def read_csv(file_path, first_codec='utf8', verbose=False, **kwargs):
    """
    A wrap to pandas read_csv with mods to accept a dataframe or filepath.
    returns dataframe untouched, reads filepath and returns dataframe based on arguments.
    """
    if isinstance(file_path, pd.DataFrame):
        return file_path
    kwargs['sep'] = kwargs.get('sep', ',')
    kwargs['low_memory'] = kwargs.get('low_memory', False)
    codecs = list(set([first_codec] + CODECS))

    for c in codecs:
        try:
            kwargs['encoding'] = c
            return pd.read_csv(file_path, **kwargs)
        except (UnicodeError,
                UnicodeDecodeError,
                UnboundLocalError) as e:
            if verbose:
                logging.info(e)
        except Exception as e:
            if 'tokenizing' in str(e):
                pass
            else:
                raise
    raise IOError("Failed to open {}.".format(file_path))


def _count(item, string):
    if len(item) == 1:
        return len(''.join(x for x in string if x == item))
    return len(str(string.split(item)))


def _identify_separator(file_path):
    """
    Identifies the separator of data in a filepath.
    It reads the first line of the file and counts supported separators.
    Currently supported separators: ['|', ';', ',','\t',':']
    """
    ext = os.path.splitext(file_path)[1].lower()
    allowed_exts = ['.csv', '.txt', '.tsv']
    assert ext in ['.csv', '.txt'], "Unexpected file extension {}. \
                                    Supported extensions {}\n filename: {}".format(
        ext, allowed_exts, os.path.basename(file_path))
    maybe_seps = ['|',
                  ';',
                  ',',
                  '\t',
                  ':']

    with open(file_path, 'r') as fp:
        header = fp.__next__()

    count_seps_header = {sep: _count(sep, header) for sep in maybe_seps}
    count_seps_header = {sep: count for sep, count in count_seps_header.items() if count > 0}

    if count_seps_header:
        return max(count_seps_header.__iter__(),
                   key=(lambda key: count_seps_header[key]))
    else:
        raise IOError(
            "Unable to identify value separator.\n"
            "Header: {}\nSeps Searched: {}".format(
             header, maybe_seps))


def read_text(filepath, **kwargs):
    """
    A wrapper to read_csv which wraps pandas.read_csv().
    The benefit of using this function is that it automatically identifies the column separator.
    .tsv files are assumed to have a \t (tab) separation
    .csv files are assumed to have a comma separation.
    .txt (or any other type) get the first line of the file opened
        and get tested for various separators as defined in the _identify_separator function.
    """
    if isinstance(filepath, pd.DataFrame):
        return filepath
    sep = kwargs.get('sep', None)
    ext = os.path.splitext(filepath)[1].lower()

    if sep is None:
        if ext == '.tsv':
            kwargs['sep'] = '\t'

        elif ext == '.csv':
            kwargs['sep'] = ','

        else:
            found_sep = _identify_separator(filepath)
            kwargs['sep'] = found_sep

    return read_csv(filepath, **kwargs)


def read_file(file_path, **kwargs):
    """
    Uses pandas.read_excel (on excel files) and returns a dataframe of the
    first sheet (unless sheet is specified in kwargs).
    Uses read_text (on .txt,.tsv, or .csv files) and returns a dataframe of the data.
    One function to read almost all types of data files.
    """
    if isinstance(file_path, pd.DataFrame):
        return file_path

    ext = os.path.splitext(file_path)[1].lower()

    if ext in ('.xlsx', '.xls'):
        kwargs.pop('dtype', None)
        return pd.read_excel(file_path, **kwargs)

    elif ext in ('.txt', '.tsv', '.csv'):
        return read_text(file_path, **kwargs)

    elif ext in ('.gz', '.bz2', '.zip', 'xz'):
        return read_csv(file_path, **kwargs)

    elif ext in ('.h5'):
        return pd.read_hdf(file_path)

    else:
        raise NotImplementedError("Unable to read '{}' files".format(ext))


def chunks(df, chunksize=None):
    """
    Yields chunks from a dataframe as large as chunksize until
    there are no records left.

    :param df: (pd.DataFrame)
    :param chunksize: (int, default None)
        The max rows for each chunk.
    :return: (pd.DataFrame)
        Portions of the dataframe
    """
    if chunksize is None or chunksize <= 0 or chunksize >= df.index.size:
        yield df
    else:
        while df.index.size > 0:
            take = df.iloc[0: chunksize]
            df = df.iloc[chunksize: df.index.size]
            yield take


def export(df, to_path, **kwargs):
    """
    A simple dataframe-export either to csv or excel.

    :param df: (pd.DataFrame)
        The dataframe to export
    :param to_path: (str)
        The filepath to export to.

    :param kwargs: (pd.to_csv/pd.to_excel kwargs)
        Look at pandas documentation for kwargs.
    :return: None
    """
    filebase, ext = os.path.splitext(to_path)
    ext = ext.lower()
    if ext is '.xlsx':
        df.to_excel(to_path, **kwargs)
    elif ext in ['.txt', '.csv']:
        df.to_csv(to_path, **kwargs)
    else:
        raise NotImplementedError("Not sure how to export '{}' files.".format(ext))


def export_chunks(df, to_path, max_size=None, overwrite=True, **kwargs):
    """
    Exports a dataframe into chunks and returns the filepaths.

    :param df: (pd.DataFrame)
        The DataFrame to export in chunks.
    :param to_path: (str)
        The base filepath (will be incremented by 1 for each export)
    :param max_size: (int, default None)
        The max size of each dataframe to export.
    :param kwargs: (pd.to_csv/pd.to_excel kwargs)
        Look at pandas documentation for kwargs.
    :return: list(filepaths exported)
    """
    paths = []
    for chunk in chunks(df, chunksize=max_size):
        export(chunk, to_path, **kwargs)
        paths.append(to_path)
        to_path = path_incremented(to_path, overwrite=overwrite)

    return paths