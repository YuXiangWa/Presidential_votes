from urllib.parse import quote
import pandas as pd
from string import ascii_uppercase

def get_tidy_data(file_path):
    # 略過合併儲存格
    xls_df = pd.read_excel(file_path, skiprows=[0, 1, 3, 4])
    column_names = list(xls_df.columns)
    n_candidates = len(column_names) - 11
    candidate_numbers_names = column_names[3:(3+n_candidates)]
    column_names = ["district", "village", "office"] + candidate_numbers_names + list(ascii_uppercase[:8])
    xls_df.columns = column_names
    # 填補行政區
    imputed_district = xls_df['district']
    imputed_district = imputed_district.fillna(method='ffill')
    xls_df = xls_df.drop('district', axis=1)
    xls_df.insert(0, 'district', imputed_district)
    # 清除行政區多餘的空白
    xls_df['district'] = xls_df['district'].str.replace('\u3000', '').str.strip()
    # 捨棄總計與小計的觀測值
    tidy_data = xls_df.dropna().reset_index(drop=True)
    return tidy_data

def get_party(number):
    if number == '1':
        party = '親民黨'
    elif number == '2':
        party = '中國國民黨'
    elif number == '3':
        party = '民主進步黨'
    return party

def get_presidential_votes():
    city_county_names = ["臺北市", "新北市", "桃園市", "臺中市", "臺南市", "高雄市", "新竹縣", "苗栗縣", "彰化縣", "南投縣", "雲林縣", "嘉義縣", "屏東縣", "宜蘭縣", "花蓮縣", "臺東縣", "澎湖縣", "基隆市", "新竹市", "嘉義市", "金門縣", "連江縣"]
    file_names = ["總統-A05-4-候選人得票數一覽表-各投開票所({}).xls".format(city_county) for city_county in city_county_names]
    file_name_urls = [quote(file_name) for file_name in file_names]
    file_paths = ["https://taiwan-election-data.s3-ap-northeast-1.amazonaws.com/presidential_2020/{}".format(file_name_url) for file_name_url in file_name_urls]
    presidential_votes = pd.DataFrame()
    for file_path, city_county in zip(file_paths, city_county_names):
        tidy_data = get_tidy_data(file_path)
        # 轉置
        tidy_data = tidy_data.drop(list(ascii_uppercase[:8]), axis=1)
        candidate_infos = list(tidy_data.columns[3:])
        long_format = pd.melt(tidy_data, id_vars=['district', 'village', 'office'], value_vars=candidate_infos, var_name="candidate_info", value_name='votes')
        long_format['city_county'] = city_county
        # 合併
        presidential_votes = presidential_votes.append(long_format)
        print("現在處理{}的資料...".format(city_county))
    # 整理
    split_candidate_info = presidential_votes["candidate_info"].str.split("\n", expand=True)
    presidential_votes["number"] = split_candidate_info[0].str.replace('\(', '').str.replace('\)', '')
    presidential_votes["candidates"] = split_candidate_info[1].str.cat(split_candidate_info[2], '/')
    presidential_votes['party'] = presidential_votes['number'].apply(get_party)
    presidential_votes = presidential_votes[["city_county", "district", "village", "office", "number", "party", "candidates", "votes"]]
    presidential_votes['number'] = presidential_votes['number'].astype(int)
    presidential_votes['office'] = presidential_votes['office'].astype(int)
    presidential_votes['votes'] = presidential_votes['votes'].astype(str)
    presidential_votes['votes'] = presidential_votes['votes'].str.replace(',', '').astype(int)
    presidential_votes = presidential_votes.reset_index(drop=True)
    return presidential_votes

presidential_votes = get_presidential_votes()
presidential_votes.head()
presidential_votes.tail()
presidential_votes.to_csv('presidential_votes.csv', index=False,encoding='utf-8-sig')