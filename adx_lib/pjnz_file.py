from rpy2.robjects import r, pandas2ri, packages
from rpy2.rinterface import NALogicalType
import pandas
import numpy
import zipfile
import io
import logging

# Setup R packages used by SpecIO to extract data.
utils = packages.importr("utils")
if not packages.isinstalled('devtools'):
    utils.install_packages('devtools')
    packages.importr("devtools")
if not packages.isinstalled('specio'):
    r['install_github']('mrc-ide/specio')
packages.importr("specio")
pandas2ri.activate()


class PJNZFile():

    # Determines the files in the PJNZ that we want to import to Pandas
    # Key is the suffix, Value specifies kwargs sent to read_csv
    file_suffixes = {
        '.DP': {'dtype': str}
    }
    year_range = map(lambda x: str(x), range(1970, 2026))
    default_columns = year_range

    def __init__(self, fpath, file_suffixes=file_suffixes, country=None):
        """
        Files are extracted upon object creation.
        """
        self.fpath = fpath
        self.fname = fpath.split('/')[-1][:-5]  # fName w/o path or extension
        self.file_suffixes = file_suffixes
        self.pjnz_file = zipfile.PyZipFile(fpath)
        self.epp_data = {}
        self.dp_tables = {}
        self.epp('subpops')  # This calculates self.epidemic_type
        if country:
            self.country = country
        else:
            self.country = fpath.split('/')[-1].split('_')[0]

        self._extract_files()

    def _extract_files(self):
        """
        This funtion takes each of the specified file_suffixes and try's to
        import it as a Pandas dataframe, using the specified kwargs.
        """
        self.dataframes = {}
        for file_suffix, kwargs in self.file_suffixes.items():
            filename = self.fname + file_suffix
            self.dataframes[filename] = pandas.read_csv(
                PJNZFile._add_delimiters(
                    self.pjnz_file.open(filename, 'r')
                ),
                **kwargs
            )

    def epp(self, table):
        """
        Uses the R package SpecIO, developed at Imperial, to import some of the
        data from the Spectrum file.
        """

        # Details the R functions available and tables of data they export.
        epp_functions = {
            "read_epp_data": [
                'anc.prev',
                'anc.n',
                'ancrtsite.prev',
                'ancrtsite.n',
                'hhs'
            ],
            "read_epp_subpops": [
                'subpops',
                'turnover'
            ]
        }

        # Utility function to convert an R matrix into a pandas dataframe
        def r2df(r_matrix):
            dataframe = pandas2ri.ri2py_dataframe(r_matrix)
            dataframe.columns = r_matrix.colnames
            dataframe.index = r_matrix.rownames
            return dataframe

        # Determines the R function to call, given the requested table
        def get_function(table):
            for function, tables in epp_functions.iteritems():
                if table in tables:
                    return function

        # Combines data for each group into one complete data set.
        def read_epp_data():
            # Import data for every table we are interested in
            # Data is stratified into groups which we have to combine.
            # Groups are either regions or sub-populations depending on epidemic type.
            epp_data = r['read_epp_data'](self.fpath)
            for table_name in epp_functions[function]:
                complete_data = {}
                for group in epp_data.names:
                    try:
                        data_frame = r2df(epp_data.rx2(group).rx2(table_name))
                        data_frame = data_frame.astype(float)
                        data_frame['Group'] = group
                        complete_data[group] = data_frame
                    except TypeError:
                        pass  # Only get the data if it exists

                if complete_data.values():
                    self.epp_data[table_name] = pandas.concat(complete_data.values())
                else:
                    self.epp_data[table_name] = None

        # Combines data for each group into one complete data set.
        def read_epp_subpops():
            epp_subpops = r['read_epp_subpops'](self.fpath)
            pops_data = {}
            turnover_data = {}
            self.epidemic_type = r['attr'](epp_subpops, 'epidemicType')[0]

            for group in epp_subpops.rx2('subpops').names:
                try:
                    # Get the population data
                    data_frame = r2df(epp_subpops.rx2('subpops').rx2(group))
                    data_frame['Group'] = group
                    pops_data[group] = data_frame
                except TypeError:
                    pass  # Only get the data if it exists

                try:
                    # Get the turnover data
                    duration = r['attr'](
                        epp_subpops.rx2('subpops').rx2(group),
                        'duration'
                    )[0]
                    if type(duration) is NALogicalType:
                        duration = numpy.NaN
                    turnover_data[group] = pandas.DataFrame({group: duration}, index=['Duration'])
                except TypeError:
                    pass  # Only get the data if it exists

            if pops_data.values():
                self.epp_data['subpops'] = pandas.concat(pops_data.values())
            else:
                self.epp_data['subpops'] = None

            if turnover_data.values():
                self.epp_data['turnover'] = pandas.concat(turnover_data.values(), axis='columns')
            else:
                self.epp_data['turnover'] = None

        # Only import if we havn't already done so.
        if self.epp_data.get(table, None) is None:
            # Call the relavent R function to get the data
            function = get_function(table)
            locals()[function]()

        # Return only the requested table
        return self.epp_data[table]

    def dp(self, tag, type=None, columns=None):
        """
        Loads a dp table stored under the specified tag. Converts the
        table to the specified type and adds the specified columns. If type
        and columns arn't specified as arguments, it will look into the
        PJNZFile's dp_tables property for the relevant configurations.
        """
        # Setup some default values
        if not type:
            type = self.dp_tables.get(tag, {}).get('type', float)
        if not columns:
            columns = self.dp_tables.get(tag, {}).get('columns', PJNZFile.default_columns)

        # DP tables are cached in the dp_tables property alongside configs
        # Only extract table if needbe as it's computationally intensive
        if self.dp_tables.get(tag, {}).get('data') is not None:
            table = self.dp_tables[tag]['data']
        else:
            table = self.extract_dp_table(tag, type, columns)

            # Cache the table
            if not self.dp_tables.get(tag):
                self.dp_tables[tag] = {}
            self.dp_tables[tag]['data'] = table
            self.dp_tables[tag]['type'] = type
            self.dp_tables[tag]['columns'] = columns

        return table

    def extract_dp_table(self, tag, type=float, columns=default_columns):
        """
        The DP file appears to be made up of a number of subsidary dataframes,
        each taggedv and labelled. There isn't a clear pattern to the way
        they are structured in the sheet, but this function broadly pulls
        out a subset of the DP sheet for a given tag.
        """
        # Get the entire DP sheet with rows and columns indexed by numbers
        dp_sheet = self.dataframes.get(self.fname + '.DP')
        if dp_sheet is None:
            raise FileNotFoundError("DP sheet not found")
        dp_sheet.columns = range(0, len(dp_sheet.columns))

        # Find desired tag in first column
        tag = "<" + str(tag) + ">"
        try:
            start_row = dp_sheet.index[dp_sheet[0] == tag].tolist()[0]
        except IndexError:
            raise ValueError(tag + " not found in DP sheet")

        # Find end tag that follows the desired tag
        end_rows = dp_sheet.index[dp_sheet[0] == "<End>"].tolist()
        for n in end_rows:
            if n > start_row:
                end_row = n
                break

        # Slice out the desired sub-table, and store the table name and tag.
        dp_table = dp_sheet.copy().iloc[start_row+2:end_row, 3:]
        dp_table.name = dp_sheet.iloc[start_row+1, 1]
        dp_table.tag = tag

        # Drop empty columns at end of data, and assign column names to the data
        dp_table = dp_table.drop(range(
            len(columns)+3,
            len(dp_table.columns)+3
        ), axis='columns')
        dp_table.columns = columns
        if "Drop" in columns:
            dp_table = dp_table.drop("Drop", 'columns')

        # Convert the type of the data
        if type:
            try:
                dp_table = PJNZFile._convert_to_type(dp_table, type)
            except Exception:
                logging.error("Can't convert " + tag + " to " + str(type))
                raise

        return dp_table

    @staticmethod
    def _add_delimiters(file_object, delimiter=','):
        """
        Pandas does not allow the import of irregular shaped CSV files - the first row must have
        more elements that any other row.  This function adds extra delimiters to the first row so
        it matches the longest row in length.  All empty cells will be assigned NaN if the Pandas
        dataframe.
        """
        s_data = ''
        max_num_delimiters = 0

        with file_object as f:
            for line in f:
                s_data += line
                delimiter_count = line.count(delimiter)
                if delimiter_count > max_num_delimiters:
                    max_num_delimiters = delimiter_count

        s_delimiters = delimiter * max_num_delimiters + '\n'

        return io.StringIO(unicode(s_delimiters + s_data, "utf-8"))

    @staticmethod
    def _convert_to_type(df, type):
        # Do an NaN safe data type conversion
        df = df.fillna(-99999999)
        df = df.astype(type)
        df = df.replace(-99999999, numpy.nan)
        return df
