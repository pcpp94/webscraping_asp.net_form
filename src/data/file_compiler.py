import os
import pandas as pd
import datetime

from ..config import COMPILED_OUTPUTS_DIR, OUTPUTS_DIR

## Getting files:
files = pd.DataFrame({"files": os.listdir(OUTPUTS_DIR)})
new_files = files["files"].str.split("_", expand=True)
new_files[0].unique()
files_df = files.copy()
files_df["report"] = (
    files_df["files"].str.split("_", expand=True)[0]
    + "_"
    + files_df["files"].str.split("_", expand=True)[1]
)

## Compiling by Reports:


def compile_Generacion1():

    df = pd.DataFrame()
    for file in files_df[files_df["report"] == "report1"]["files"].tolist():
        aux = pd.read_csv(os.path.join(OUTPUTS_DIR, file), index_col=0)
        aux["report"] = "peak"
        aux["filename"] = file
        df = pd.concat([df, aux])
    df = df[~df["0"].isna()]
    df = df[~df["0"].isin(["Plant ", "Output"])]
    df = df.rename(columns={"0": "power_station", "1": "peak_power_mw"})
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
    df["peak_power_mw"] = pd.to_numeric(df["peak_power_mw"])

    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "daily_Generacion.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "daily_Generacion.parquet"))


def compile_Generacion2():

    df = pd.DataFrame()
    for file in files_df[files_df["report"] == "report2"]["files"].tolist():
        aux = pd.read_csv(os.path.join(OUTPUTS_DIR, file), index_col=0)
        aux["report"] = "hourly_Generacion"
        aux["filename"] = file
        df = pd.concat([df, aux])
    df = df.drop(columns=["Hr", "Total Gross", "Aux"])
    variables = df.columns.tolist()
    for val in ["date", "hour", "report", "filename"]:
        variables.remove(val)
    df = pd.melt(
        df,
        id_vars=["date", "hour", "report", "filename"],
        value_vars=variables,
        var_name="variable",
        value_name="Generacion_mwh",
    )
    df.columns = df.columns.str.lower()
    df["plant"] = df["variable"].str.split(" ", expand=True)[0]
    df["type"] = df["variable"].str.split(" ", expand=True)[1]
    df = df.drop(columns="variable")
    to_change = df[df["Generacion_mwh"] == "-"].index
    df.loc[to_change, "Generacion_mwh"] = 0
    df["Generacion_mwh"] = pd.to_numeric(df["Generacion_mwh"])
    df = df[~df["Generacion_mwh"].isna()]
    df["datetime"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["hour"].astype(str), format="%d/%m/%Y %H"
    )
    df["date"] = pd.to_datetime(df["date"].astype(str), format="%d/%m/%Y")
    df = df[~df["type"].isna()]
    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "hourly_Generacion.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "hourly_Generacion.parquet"))


def compile_load():

    df = pd.DataFrame()
    for file in files_df[files_df["report"] == "report21"]["files"].tolist():
        aux = pd.read_csv(os.path.join(OUTPUTS_DIR, file), index_col=0)
        aux["report"] = "hourly_demand"
        aux["filename"] = file
        df = pd.concat([df, aux])
    variables = df.columns.tolist()
    for val in ["date", "hour", "report", "filename"]:
        variables.remove(val)
    df = pd.melt(
        df,
        id_vars=["date", "hour", "report", "filename"],
        value_vars=variables,
        var_name="cat",
        value_name="kwh",
    )
    df.columns = df.columns.str.lower()
    to_change = df[df["kwh"] == "-"].index
    df.loc[to_change, "kwh"] = 0
    df["kwh"] = pd.to_numeric(df["kwh"])
    df = df[~df["kwh"].isna()]
    df["datetime"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["hour"].astype(str), format="%d/%m/%Y %H"
    )
    df["date"] = pd.to_datetime(df["date"].astype(str), format="%d/%m/%Y")
    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "hourly_demand.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "hourly_demand.parquet"))


def get_country_df(aux: pd.DataFrame, aux_1) -> pd.DataFrame:
    country_df = aux.loc[:aux_1,]
    country_df = country_df.drop(
        columns=country_df.columns[country_df.loc[0, :].isin(["Lowest", "Largest"])]
    )
    country_df.columns = country_df.loc[0, :]
    country_df = country_df.drop(index=[0, 1])
    country_df = country_df.rename(columns={country_df.columns[-1]: "date"})
    country_df = country_df[~country_df["System"].str.contains("Temp")]
    country_df = pd.melt(
        country_df,
        id_vars=["System", "date"],
        value_vars=[
            "Lowest Demand Load",
            "At Peak Demand Load",
        ],
        var_name="system_measurement",
        value_name="nominal_value",
    )
    country_df = country_df[country_df["System"] != "Date"]
    country_df["nominal_value"] = country_df["nominal_value"].str.replace(":", ".")
    country_df["nominal_value"] = pd.to_numeric(country_df["nominal_value"])
    country_df["date"] = pd.to_datetime(
        country_df["date"].astype(str), format="%d/%m/%Y"
    )
    country_df = country_df.rename(
        columns={"System": "variable", "system_measurement": "system"}
    )
    country_df["peak_min_flag"] = country_df["system"].str.split(" ", expand=True)[1]
    country_df["system"] = country_df["system"].apply(
        lambda x: " ".join(x.split(" ")[2:])
    )
    return country_df


def get_areas_df(aux: pd.DataFrame, aux_2, break_points) -> pd.DataFrame:
    areas = aux.loc[break_points["Area"] : aux_2, :]
    areas = areas[areas["0"] != "Area Demanda"]
    areas.columns = [
        "Min MW",
        "Min Time",
        "MW Lowest",
        "category",
        "date",
    ]
    areas.columns = areas.columns.str.lower()
    areas.columns = areas.columns.str.replace(" ", "_")
    areas = pd.melt(
        areas,
        id_vars=["category", "date"],
        value_vars=[
            "min_mw",
            "min_time",
            "max_mw",
        ],
        var_name="measured_variable",
        value_name="nominal_value",
    )
    areas["grouping"] = "Area Demanda"
    areas["nominal_value"] = areas["nominal_value"].str.replace(":", ".")
    areas["nominal_value"] = pd.to_numeric(areas["nominal_value"])
    areas["date"] = pd.to_datetime(areas["date"].astype(str), format="%d/%m/%Y")
    return areas


def get_gsps_df(aux: pd.DataFrame, aux_3, break_points) -> pd.DataFrame:
    gsps = aux.loc[break_points["gsps"] : aux_3, :]
    gsps = gsps[gsps["0"] != "gsps"]
    gsps.columns = [
        "Min MW",
        "Min Time",
        "MW Lowest",
        "category",
        "date",
    ]
    gsps.columns = gsps.columns.str.lower()
    gsps.columns = gsps.columns.str.replace(" ", "_")
    gsps = pd.melt(
        gsps,
        id_vars=["category", "date"],
        value_vars=[
            "min_mw",
            "min_time",
        ],
        var_name="measured_variable",
        value_name="nominal_value",
    )
    gsps["grouping"] = "gsps"
    gsps["nominal_value"] = gsps["nominal_value"].str.replace(":", ".")
    gsps["nominal_value"] = pd.to_numeric(gsps["nominal_value"])
    gsps["date"] = pd.to_datetime(gsps["date"].astype(str), format="%d/%m/%Y")
    return gsps


def get_Generacion_df(aux: pd.DataFrame, aux_4, break_points) -> pd.DataFrame:
    Generacion = aux.loc[break_points["Generacion"] : aux_4, :]
    Generacion = Generacion[Generacion["0"] != "Generacion"]
    Generacion.columns = [
        "Min MW",
        "Min Time",
        "MW Lowest",
        "category",
        "date",
    ]
    Generacion.columns = Generacion.columns.str.lower()
    Generacion.columns = Generacion.columns.str.replace(" ", "_")
    Generacion = pd.melt(
        Generacion,
        id_vars=["category", "date"],
        value_vars=[
            "min_mw",
            "min_time",
            "mw_at_min",
            "mw_at_max",
        ],
        var_name="measured_variable",
        value_name="nominal_value",
    )
    Generacion["grouping"] = "Generacion"
    Generacion["nominal_value"] = Generacion["nominal_value"].str.replace(":", ".")
    Generacion["nominal_value"] = pd.to_numeric(Generacion["nominal_value"])
    Generacion["date"] = pd.to_datetime(
        Generacion["date"].astype(str), format="%d/%m/%Y"
    )
    return Generacion


def get_mining_df(aux: pd.DataFrame, break_points) -> pd.DataFrame:
    mining = aux.loc[break_points["MINE"] :, :]
    mining = mining[mining["0"] != "MINE"]
    mining.columns = [
        "Min MW",
        "Min Time",
        "MW Lowest",
        "category",
        "date",
    ]
    mining.columns = mining.columns.str.lower()
    mining.columns = mining.columns.str.replace(" ", "_")
    mining = pd.melt(
        mining,
        id_vars=["category", "date"],
        value_vars=[
            "min_mw",
            "min_time",
            "mw_at_min",
            "mw_at_max",
            "max_mw",
        ],
        var_name="measured_variable",
        value_name="nominal_value",
    )
    mining["grouping"] = "MINE"
    mining["nominal_value"] = mining["nominal_value"].str.replace(":", ".")
    mining["nominal_value"] = pd.to_numeric(mining["nominal_value"])
    mining["date"] = pd.to_datetime(mining["date"].astype(str), format="%d/%m/%Y")
    return mining


def compile_Resumen():

    df = pd.DataFrame()
    df_2 = pd.DataFrame()

    for file in files_df[files_df["report"] == "Resumen_report1"]["files"].tolist():
        aux = pd.read_csv(os.path.join(OUTPUTS_DIR, file), index_col=0)
        aux = aux.drop(
            columns=aux.columns[aux.loc[1, :].isin(["This Year", "Last Week"])]
        )
        aux = aux.drop(columns=min(aux.columns[aux.loc[1, :] == "System"]))
        aux = aux[aux.loc[:, "0"].str.contains("Station") != True]
        aux = aux[aux.loc[:, "0"].str.contains("SOUTHERN") != True]
        from_drop = aux[aux.loc[:, "0"].str.contains("Note") == True].index[0] - 1
        aux = aux.loc[:from_drop, :]
        splits = ["Area", "gsps", "Generacion", "MINE"]
        location = []
        for item in splits:
            location.append(aux[aux.loc[:, "0"].str.startswith(item) == True].index[0])
        break_points = dict(zip(splits, location))
        aux_1 = break_points["Area"] - 1
        aux_2 = break_points["gsps"] - 1
        aux_3 = break_points["Generacion"] - 1
        aux_4 = break_points["MINE"] - 1

        country_df_aux = get_country_df(aux, aux_1)
        areas = get_areas_df(aux, aux_2, break_points)
        gsps = get_gsps_df(aux, aux_3, break_points)
        Generacion = get_Generacion_df(aux, aux_4, break_points)
        mining = get_mining_df(aux, break_points)
        individuals_df_aux = pd.concat([areas, gsps, Generacion, mining])
        country_df_aux["report"] = "PEResumen"
        country_df_aux["filename"] = file
        individuals_df_aux["report"] = "PEResumen"
        individuals_df_aux["filename"] = file

        df = pd.concat([df, country_df_aux])
        df_2 = pd.concat([df_2, individuals_df_aux])

    df = df.reset_index(drop=True)
    df["peak_min_flag"] = df["peak_min_flag"].apply(
        lambda x: "Lowest" if x == "Minimum" else x
    )
    df_2 = df_2.reset_index(drop=True)
    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_country.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_country.parquet"))
    df_2.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_by_category.csv"))
    df_2.to_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_by_category.parquet")
    )
    print("Compiled Resumen Countries and Categories Report")


def compile_Cuzco():

    df = pd.DataFrame()
    df_2 = pd.DataFrame()

    for file in files_df[files_df["report"] == "Resumen_report4"]["files"].tolist():
        aux = pd.read_csv(os.path.join(OUTPUTS_DIR, file), index_col=0)
        aux.columns = [
            "min_mw",
            "min_time",
            "drop",
            "drop",
            "mw_at_min",
            "category",
            "mw_at_max",
            "drop",
            "drop",
            "max_mw",
            "max_time",
            "date",
        ]
        aux = aux.drop(columns="drop")
        aux = pd.melt(
            aux,
            id_vars=["category", "date"],
            value_vars=[
                "min_mw",
                "min_time",
                "mw_at_min",
                "mw_at_max",
                "max_mw",
                "max_time",
            ],
            var_name="measured_variable",
            value_name="nominal_value",
        )
        aux["grouping"] = "Cuzco"
        try:
            aux["nominal_value"] = aux["nominal_value"].replace(":", ".", regex=True)
            aux["nominal_value"] = pd.to_numeric(aux["nominal_value"])
        except:
            pass
        aux["date"] = pd.to_datetime(aux["date"].astype(str), format="%d/%m/%Y")
        aux["filename"] = file
        df = pd.concat([df, aux])

    for file in files_df[files_df["report"] == "Resumen_report6"]["files"].tolist():
        aux2 = pd.read_csv(os.path.join(OUTPUTS_DIR, file), index_col=0)
        aux2.columns = [
            "category",
            "mw_at_max(country_load)",
            "drop",
            "drop",
            "date",
        ]
        aux2 = aux2.drop(columns="drop")
        aux2 = pd.melt(
            aux2,
            id_vars=["category", "date"],
            value_vars=["mw_at_max(country_load)"],
            var_name="measured_variable",
            value_name="nominal_value",
        )
        aux2["grouping"] = "Cuzco"
        try:
            aux2["nominal_value"] = aux2["nominal_value"].replace(":", ".", regex=True)
            aux2["nominal_value"] = pd.to_numeric(aux2["nominal_value"])
        except:
            pass
        aux2["date"] = pd.to_datetime(aux2["date"].astype(str), format="%d/%m/%Y")
        aux2["filename"] = file
        df_2 = pd.concat([df_2, aux2])

    df = pd.concat([df, df_2])
    df["report"] = "Cuzco"
    df = df.reset_index(drop=True)
    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_Cuzco.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_Cuzco.parquet"))
    print("Compiled Resumen Cuzco Report")


def compile_SOUTHERN():

    df = pd.DataFrame()
    df_2 = pd.DataFrame()

    for file in files_df[files_df["report"] == "Resumen_report5"]["files"].tolist():
        aux = pd.read_csv(os.path.join(OUTPUTS_DIR, file), index_col=0)
        aux.columns = ["substation", "value", "date"]
        aux = aux[aux["substation"].str.lower() != "total"]
        aux["grouping"] = "SOUTHERN substations"
        aux["filename"] = file
        aux["report"] = "SOUTHERN substations"
        df = pd.concat([df, aux])

    for file in files_df[files_df["report"] == "Resumen_report7"]["files"].tolist():
        aux2 = pd.read_csv(os.path.join(OUTPUTS_DIR, file), index_col=0)
        aux2.columns = ["substation", "value", "date"]
        aux2 = aux2[aux2["substation"].str.lower() != "total"]
        aux2["grouping"] = "SOUTHERN substations"
        aux2["filename"] = file
        aux2["report"] = "SOUTHERN substations"
        df_2 = pd.concat([df_2, aux2])

    df = df.merge(
        df_2[["substation", "value", "date"]].rename(columns={"value": "value_2"}),
        how="left",
        on=["substation", "date"],
    )
    df["value"] = df[["value", "value_2"]].mean(axis=1)
    df = df.drop(columns=["value_2"])
    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_SOUTHERN_ss.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_SOUTHERN_ss.parquet"))
    print("Compiled Resumen SOUTHERN SS Report")


def Resumen_merge():

    df = pd.read_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_by_category.parquet")
    )
    df2 = pd.read_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_country.parquet")
    )
    df3 = pd.read_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_Cuzco.parquet")
    )

    df2["grouping"] = "Countries"
    df2["measured_variable"] = df2["variable"] + "_" + df2["peak_min_flag"]
    df2 = df2.drop(columns=["variable", "peak_min_flag"]).rename(
        columns={"system": "category"}
    )
    merge_df = df2[["category", "measured_variable"]].drop_duplicates().copy()
    merge_df["new"] = [
        "mw_at_min",
        "min_time",
        "min_freq",
        "mw_at_max",
        "max_time",
        "max_freq",
        "max_freq",
    ]
    df2 = (
        df2.merge(merge_df, how="left", on=["category", "measured_variable"])
        .drop(columns=["measured_variable"])
        .rename(columns={"new": "measured_variable"})
    )

    df = pd.concat([df, df2]).reset_index(drop=True)
    df = df[df["category"] != "SOUTHERN"]
    df = pd.concat([df, df3])
    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_all.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "pe_Resumen_all.parquet"))
    print("Merged Resumen Reports")


def compile_all():
    compile_Generacion1()
    compile_Generacion2()
    compile_load()
    compile_Resumen()
    compile_Cuzco()
    compile_SOUTHERN()
    Resumen_merge()


if __name__ == "__main__":
    compile_all()
