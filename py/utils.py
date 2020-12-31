
# returns list of full names from a column header
def get_full_name_list_from_cols(df_meta, col_names):
    cols = []
    for col in col_names:
        if col == 'GEOID' or col == 'geometry':
            continue
        meta = df_meta.loc[df_meta['Short_Name'] == col]
        col_full = meta.at[meta.index.values[0], 'Full_Name']
        cols.append(col_full)
    return cols


def get_col_map_to_names(df_meta, col_names):
    cols = {}
    for col in col_names:
        if col == 'GEOID' or col == 'geometry':
            continue
        meta = df_meta.loc[df_meta['Short_Name'] == col]
        col_full = meta.at[meta.index.values[0], 'Full_Name']
        cols[col] = col_full
    return cols


def get_name_list_from_cols(df_meta, col_names):
    cols = []
    for col in col_names:
        if col == 'GEOID' or col == 'geometry':
            continue
        meta = df_meta.loc[df_meta['Short_Name'] == col]
        cols.apped(meta.at[meta.index.values[0], 'Full_Name'])
    return cols

def get_full_name_map_from_cols(df_meta, col_names):
    cols = {}
    for col in col_names:
        if col == 'GEOID' or col == 'geometry':
            continue
        meta = df_meta.loc[df_meta['Short_Name'] == col]
        col_full = meta.at[meta.index.values[0], 'Full_Name']
        cols[col_full] = col
    return cols


def remove_margin_of_error(columns):
    removed_cols = []
    cols = {}
    for name, col in columns.items():
        if 'Margin of Error' in name:
            removed_cols.append(col)
        else:
            cols[name] = col
    return cols, removed_cols


def get_margin_of_error_list(df_columns):
    removed_cols = []
    for col in df_columns:
        if 'Margin of Error' in col:
            removed_cols.append(col)
    return removed_cols


