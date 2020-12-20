
# returns list of full names from a column header
def get_full_name_from_cols(df_meta, col_names):
    cols = []
    for col in col_names:
        if col == 'GEOID' or col == 'geometry':
            continue
        meta = df_meta.loc[df_meta['Short_Name'] == col]
        col_full = meta.at[meta.index.values[0], 'Full_Name']
        cols.append(col_full)
    return cols