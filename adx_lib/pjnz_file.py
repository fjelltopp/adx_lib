import rpy2.robjects.packages as rpackages
from rpy2.robjects import r, pandas2ri
import pandas
import numpy
import zipfile
import io
import re
import logging

# Setup R packages to import the data
utils = rpackages.importr("utils")
if not rpackages.isinstalled('devtools'):
    utils.install_packages('devtools')
    rpackages.importr("devtools")
if not rpackages.isinstalled('specio'):
    r['install_github']('mrc-ide/specio')
rpackages.importr("specio")
pandas2ri.activate()



class PJNZFile():

    # Determines the files in the PJNZ that we want to import to Pandas
    # Key is the suffix, Value specifies kwargs sent to read_csv
    file_suffixes = {
        '.DP': {'dtype': str}
    }
    surv_file_datasheets = [
        #"POPULATION SIZES for:",
        #"ANC-SS DATA for:",
        "ANC-SS DATA (PREV & NUMBER SEPARATE) for:",
        #"ANC-RT DATA for:",
        "ANC-RT DATA (PREV & NUMBER SEPARATE) for:",
        #"ANC-RT CENSUS DATA for:",
        #"SURVEY DATA for:",
    ]
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

    def _extract_epp_data(self):
        """
        Uses the R package SpecIO, developed at Imperial, to import some of the
        data from the Spectrum file.
        """
        self.epp_data = r['read_epp_data'](self.fpath)
        return self.epp_data

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

    def extract_surv_data(self):
        """
        The ANC Sentinel Surveillance and Routine Testing data are stored in
        the surv file.  This breaks the surv data down in to sub dataframes
        using the =========== and ---------- dividers.
        """
        # Get entire surv.csv sheet with rows and columns indexed by numbers
        surv_sheet = self.dataframes.get(self.fname + '_surv.csv')
        if surv_sheet is None:
            raise FileNotFoundError("surv.csv sheet not found")
        surv_sheet.columns = range(0, len(surv_sheet.columns))

        tags = "|".join(PJNZFile.surv_file_datasheets)
        dividing_rows = surv_sheet.index[
            surv_sheet[0].str.contains(tags, na=False, regex=True)
        ].tolist()
        dataframes = {}
        for index, row in enumerate(dividing_rows):
            start_row = row+1
            try:
                end_row = dividing_rows[index+1]
            except IndexError:
                break
            table = surv_sheet.copy().iloc[start_row:end_row, 0:]
            dataframes[surv_sheet.iloc[row, 0][:-5]] = table

        # # Divide up the data into multiple smaller data frames
        # tags = "=============================|------------------------------"
        # dividing_rows = surv_sheet.index[
        #     surv_sheet[0].str.contains(tags, na=False, regex=True)
        # ].tolist()
        # dataframes = []
        # for index, row in enumerate(dividing_rows):
        #     start_row = row+1
        #     try:
        #         end_row = dividing_rows[index+1]
        #     except IndexError:
        #         break
        #     table = surv_sheet.copy().iloc[start_row:end_row, 0:]
        #     dataframes.append(table)
        print(dataframes.keys())
        return dataframes

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
        df = df.fillna(-9999)
        df = df.astype(type, errors='ignore')
        df = df.replace(-9999, numpy.nan)
        return df
