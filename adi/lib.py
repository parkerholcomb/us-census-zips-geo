import pandas as pd
import plotly.express as px

### 

def get_state_codes() -> list:
    return list(pd.read_csv("state_codes.csv")['state'])

def us_adi_zip5_stats(us_adi: pd.DataFrame) -> pd.DataFrame:
    us_zips = pd.read_feather("data/us_zips.feather")
    df = us_adi.groupby(['_zip5']).agg(
        count=("_adi","count"),
        min=("_adi","min"),
        max=("_adi","max"),
        adi_mean=("_adi","mean"),
        std=("_adi","std"),
    ).reset_index()
    return df.merge(us_zips).dropna()


## once extracted

def _features_only(df): return df[[name for name in df.columns if name.startswith("_") ]]

def _load_adi_by_state(state = "TX", features_only=True):
    df = pd.read_parquet(f"data/tables/{state}.parquet")
    df = df[df['TYPE'].isna()] # this is only the "standard zip codes"
    df['_zip3'] = df['ZIPID'].apply(lambda x: x[1:4])
    df['_zip5'] = df['ZIPID'].apply(lambda x: x[1:6])
    def _to_adi(adi_natrank: str) -> int:
        try:
            return int(adi_natrank)
        except Exception as e:
            pass
    df['_adi'] = df['ADI_NATRANK'].apply(_to_adi)
    df['_state'] = state
    df = df[['_state','_zip5','_zip3','_adi']]
    return _features_only(df) if features_only else df

# state="IN"
def load_adi_all_states(force=False):
    if force:
        state_codes = get_state_codes()
        states=[]
        for state in state_codes:
            print(state)
            df = _load_adi_by_state(state)
            states.append(df)
        df = pd.concat(states).reset_index(drop=True)
        df.to_feather("data/us_adi.feather")
    else:
        df = pd.read_feather("data/us_adi.feather")
    return df