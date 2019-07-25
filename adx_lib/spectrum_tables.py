from collections import OrderedDict
from schemed_table import SchemedTable
from rpy2.robjects import pandas2ri
import pandas
import numpy as np
import pandas as pd
import logging

# Imports below are used by python snippets in the json schemas.
# They may look unused, but they actually are used!
from datetime import datetime


def build_spectrum_table(spectrum_file, schema, index=None, **kwargs):
        """
        This function factorises out common code required to auto-populate
        an ADR Spectrum resource from a PJNZ file. It uses the ADR resource
        validation schema to build a dataframe and insert into it data from
        the PJNZ file.

        IMPORTANT - This function evaluates snippets of code from the JSON
        schemas. This is not ideal, as the snippets would ideally be brought
        into the Python ecosystem. However, for the time being it was seen as
        the cleanest way to store the complex mapping of data from PJNZ to ADR
        resource.
        """
        # We reference the spectrum file from json schemas - give it a shorthand ref
        sf = spectrum_file

        # Remove the first schema field as this is the header/index
        schema = schema.copy()
        first_field = schema['fields'].pop(0)

        # Assemble the populated data file in dictionaries
        new_table = OrderedDict()
        for field in schema['fields']:
            if field.get('spectrum_file_key', False):

                # Fill row in with spectrum data
                try:
                    # IMPORTANT - We evaluate a snippet of code from the JSON file
                    data_series = list(eval(field['spectrum_file_key']))
                except Exception:
                    logging.error(
                        "Failed to evaluate " + field['name'] +
                        " spectrum_file_key: " + field['spectrum_file_key']
                    )
                    raise
                new_table[field['name']] = data_series

            else:
                # If no spectrum_file_key given, then leave series empty
                new_table[field['name']] = []

        # Fill in empty series with NAN (must match other series length)
        max_length = max([len(x) for x in new_table.values()])
        for key, value in new_table.iteritems():
            if len(value) == 0:
                new_table[key] = [np.NaN]*max_length

        new_table = pd.DataFrame.from_dict(
            new_table,
            **kwargs
        )

        # Fix the indicies if they are mannually specified
        if index:
            new_table.index = index
        # Fix the indicies if they are specified with a spectrum_file_key
        elif first_field.get('spectrum_file_key', False):
            new_table.index = list(eval(first_field['spectrum_file_key']))
        new_table.insert(0, first_field['name'], new_table.index)

        return new_table


class SizeTable(SchemedTable):

    def create_table(self, spectrum_file):
        """
        Conc Prevelance table is taken from the XML file, and loaded through
        Imperial's SpecIO R Package.
        """
        subpops = spectrum_file.epp('subpops').pivot(
            index='Group',
            columns='year',
            values='pop15to49'
        )
        subpops.columns = subpops.columns.astype(str)
        subpops['Group'] = subpops.index
        spectrum_file.epp_data['subpops'] = subpops

        return build_spectrum_table(
            spectrum_file,
            self.schema
        )


class TurnoverTable(SchemedTable):

    def create_table(self, spectrum_file):
        """
        Conc Prevelance table is taken from the XML file, and loaded through
        Imperial's SpecIO R Package.
        """

        print(spectrum_file)


class HHTable(SchemedTable):

    def create_table(self, spectrum_file):
        """
        Conc Prevelance table is taken from the XML file, and loaded through
        Imperial's SpecIO R Package.
        """

        return build_spectrum_table(
            spectrum_file,
            self.schema
        )


class ConcPrevalenceTable(SchemedTable):

    def create_table(self, spectrum_file):
        """
        Conc Prevelance table is taken from the XML file, and loaded through
        Imperial's SpecIO R Package.
        """
        # Use the SpecIO package to extrac epp model data.
        epp_data = spectrum_file.epp()
        regions = epp_data.names
        anc_prev = {}

        # Utility function to convert an R matrix into a pandas dataframe
        def _r2df(r_matrix):
            dataframe = pandas2ri.ri2py_dataframe(r_matrix)
            dataframe.columns = r_matrix.colnames
            dataframe.index = r_matrix.rownames
            return dataframe

        # Data is stored by region in the SpecIO outputs.
        for region in regions:
            data_types = epp_data.rx2(region).names
            print(data_types)
            print(_r2df(epp_data.rx2(region).rx2('ancrtcens')))
            print(epp_data.rx2(region).rx2('anc.used'))


class ANCPrevalenceTable(SchemedTable):

    def create_table(self, spectrum_file):
        """
        ANC Prevelance table is taken from the XML file, and loaded through
        Imperial's SpecIO R Package.  It is quite different to the other tables
        as data of different types from different tables are merged together
        into a single table.  This means we first have to mannually build the
        combined data source, before building the spectrum table.
        """
        # Use the SpecIO package to extrac epp model data.
        combined_anc = {
            "ANC-SS (%)": spectrum_file.epp('anc.prev'),
            "ANC-SS (N)": spectrum_file.epp('anc.n'),
            "ANC-RT (%)": spectrum_file.epp('ancrtsite.prev'),
            "ANC-RT (N)": spectrum_file.epp('ancrtsite.n')
        }

        # Filtering out empty values and merging data types into one table
        combined_anc = {k: v for k, v in combined_anc.items() if v is not None}
        for key, value in combined_anc.iteritems():
                value['Type'] = key
        combined_anc = pandas.concat(combined_anc.values())
        combined_anc['Site'] = combined_anc.index

        # Many values have been set to some huge negative figure.
        # Assume these should be empty?
        tmp = combined_anc._get_numeric_data()
        tmp[tmp < 0] = np.NaN

        spectrum_file.epp_data['combined_anc'] = combined_anc

        return build_spectrum_table(
            spectrum_file,
            self.schema
        )


class ANCTestingTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "ANCTestingValues MV": {
                "type": int
            }
        }

        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        ).dropna(how='all', axis='columns')


class BreastfeedingTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "InfantFeedingOptions MV": {
                "type": float
            }
        }
        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient="columns",
            index=self.schema['fields'][0]['example_values']
        )


class PMTCTTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "ARVRegimen MV2": {
                "type": int,
                "columns": ["Drop"] + spectrum_file.default_columns
            }
        }

        # We only want to return a subset of the columns
        columns_to_keep = [self.schema['fields'][0]['name']]
        columns_to_keep += self.schema['fields'][0]["example_values"]
        columns_to_keep = list(map(str, columns_to_keep))

        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        )[columns_to_keep]


class ARTTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "HAARTBySex MV": {"type": int},
            "MedianCD4 MV": {"type": float},
            "PercLostFollowup MV": {"type": int},
            "CD4ThreshHoldAdults MV": {"type": int},
            "ChildARTCalc MV2": {"type": float},
            "ChildTreatInputs MV3": {"type": float},
            "PercLostFollowupChild MV": {"type": int},
            "CD4ThreshHold MV": {"type": int},
            "ChildNeedPMTCT MV": {"type": float},
            "ChildOnPMTCT MV": {"type": int},
            "NumNewARTPats MV": {"type": float},
            "MedCD4CountInit MV": {"type": int}
        }
        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        )


class CaseMortalityTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "FitIncidence MV6": {"type": int}
        }
        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        )


class KnownStatusTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "ViralSuppressionInput MV": {"type": int}
        }
        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        )
