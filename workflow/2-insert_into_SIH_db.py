# %%
from tqdm import tqdm
from pathlib import Path
from pysus import SIH
import pandas as pd
import sys
sys.path.append("..")

from pyopensus.storage.whandler_sus import HandlerSIH

def load_SIH_file(data_filename):
    """Load a SIH RD/CE parquet file, add metadata columns, and parse date fields.
    
    Parameters
    ----------
    data_filename : str or pathlib.Path
        Path to the parquet file to load.
    
    Returns
    -------
    pandas.DataFrame
        Loaded data with `FONTE`, `SEQUENCIA`, and parsed date columns.
    """
    
    prefix = Path(data_filename).stem[:2]
    uf = Path(data_filename).stem[2:4]
    mes_ano=Path(data_filename).stem
    mes_ano=mes_ano.replace(prefix,"").replace(uf,"")
   
    df = pd.read_parquet(data_filename, engine="fastparquet")
    nrows = df.shape[0]
    
    df['FONTE'] = [mes_ano for n in range(nrows)]
    
    # 'SEQUENCIA' tem valores repetidos
    df['SEQUENCIA'] = df['SEQUENCIA'].str.replace(" ","0")
    # df['FONTE_SEQUENCIA']= df['FONTE']+df['SEQUENCIA']
    
    # date columns...
    if prefix != "SP":
        for col in ["NASC", "DT_INTER", "DT_SAIDA"]:
            df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors='coerce')
    return df

sih = SIH().load()
# %%
data_folder =  Path.home().joinpath("Workspace", "pyopensus", "data")
base_folder = Path.joinpath(data_folder,"sihsus")
warehouse_location = Path.joinpath(data_folder, "opendatasus")
warehouse_name = "SIHSUS_NORDESTE_NO_SERVICE.db"
warehouse_injector = HandlerSIH(warehouse_location, warehouse_name)

# warehouse_name = "SIHSUS_NORDESTE_NO_SERVICE.db"
parquet_location = base_folder


prefix_list = ["RD", "RJ", "SP"]
uf_list = ["CE"]
year_list = [2009]
month_list = [1]

files = sih.get_files(prefix_list, uf=uf_list, year=year_list, month=month_list);
filenames = [ parquet_location.joinpath(str(f).replace("dbc", "parquet")) for f in files]
for f in filenames:
    print(f, f.exists())

df = load_SIH_file(filenames[0])
print(len(df.columns))
# %%    
for current_file in tqdm(filenames):
    fname = current_file.stem
    prefix = fname[:2]
    cur_df = load_SIH_file(current_file)
    warehouse_injector.insert_sih(cur_df, fname, prefix)