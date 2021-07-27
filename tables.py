import dash_table
import pandas as pd


def generate_dash_table(df: pd.DataFrame, id, sorting=[], dropdowns={}, num_elements=10):
    """Takes a pandas DataFrame and an id an returns a table object.
       Sorting and dropdowns are optional."""
    columns = []

    for c in df.columns:
        t = {"name": c, "id": c}
        if c in dropdowns.keys():
            t['presentation'] = 'dropdown'
        columns.append(t)

    if sorting:
        df = df.sort_values(by=sorting[0]["column_id"],
                            ascending=(sorting[0]["direction"] != "desc"))

    table_data = df.to_dict('records')[:num_elements]

    dropdown = {}
    for key, val in dropdowns.items():
        e = {"options": [{'label': i, 'value': i} for i in val]}
        dropdown[key] = e

    table = dash_table.DataTable(
        id=id,
        columns=columns,
        data=table_data,
        editable=True,
        row_deletable=True,
        sort_action="native",
        sort_by=sorting,
        dropdown=dropdown,
        css=[{"selector": ".Select-menu-outer", "rule": "display: block !important"}]
    )
    return table
