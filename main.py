"""

    """

import pandas as pd
from githubdata import GithubData
from mirutil import utils as mu
import asyncio
from mirutil import async_requests as areq
from persiantools.jdatetime import JalaliDate


class RepoUrls :
    src1 = 'https://github.com/imahdimir/d-funds-SEO_RegNo-cfi_seo_ir'

ru = RepoUrls()

class Urls :
    base = 'http://cdn.tsetmc.com/api/Fund/GetFundInDetail/'

url = Urls()

def main() :
    pass

    ##
    rp_src = GithubData(ru.src1)
    df = rp_src.read_data()

    ##
    df1 = df[['SEORegisterNo' , 'Name']]

    ##
    df1['url'] = url.base + df1['SEORegisterNo'].astype(str)

    ##
    cis = mu.return_clusters_indices(df1)
    list(df1.index[0 :1])

    fu = areq.get_reps_jsons_async
    ##
    for se in cis :
        si = se[0]
        ei = se[1] + 1
        print(se)

        inds = df1.index[si :ei]

        urls = df1.loc[inds , 'url']

        out = asyncio.run(fu(urls))

        df1.loc[inds , 'json'] = out

        # break

    ##
    df2 = df1[df1['json'].isna()]
    cis = mu.return_clusters_indices(df2)

    ##
    for se in cis :
        si = se[0]
        ei = se[1] + 1
        print(se)

        inds = df2.index[si :ei]

        urls = df1.loc[inds , 'url']

        out = asyncio.run(fu(urls))

        df1.loc[inds , 'json'] = out

        # break

    ##
    df3 = df1.copy()

    ##
    df3 = df3.dropna()

    ##
    df4 = df3.explode('json')

    ##
    sr1 = df4['json'].drop_duplicates()

    ##
    df1 = df1.dropna()

    ##
    df1['data'] = df1['json'].apply(lambda x : x['fund'])

    ##
    df2 = df1.explode('data')
    sr1 = df2['data'].drop_duplicates()

    ##
    df1['stats'] = df1['data'].apply(lambda x : x['stats'])
    ##
    df1 = df1.explode('stats')

    ##
    df2 = df1.explode('stats')
    sr1 = df2['stats'].drop_duplicates()

    ##
    for el in sr1 :
        print('"' + el + '":None,')

    ##
    cols = {
            "recordDate" : None ,
            "navSub"     : None ,
            "netAsset"   : None ,
            "navStat"    : None ,
            "navRed"     : None ,
            }

    for col in cols.keys() :
        df1[col] = df1['stats'].apply(
            lambda x : x[col] if col in x.keys() else None
            )

    ##
    df1['Date'] = pd.to_datetime(df1['recordDate']).dt.date

    ##
    df1 = df1.dropna(subset = 'Date')

    ##
    df1['JDate'] = df1['Date'].apply(lambda x : JalaliDate.to_jalali(x))

    ##
    df2 = df1.sort_values('JDate')

    ##
    df2['JMonth'] = df2['JDate'].astype(str).str[:7]

    ##
    df3 = df2.groupby(['SEORegisterNo' , 'JMonth']).tail(1)

    ##
    df4 = df3[['SEORegisterNo' , 'Name' , 'JMonth' , 'netAsset' , 'navSub' ,
               'navStat' , 'navRed']]

    ##
    df4.to_excel('temp.xlsx' , index = False)


    ##

##
if __name__ == "__main__" :
    main()

##
# noinspection PyUnreachableCode
if False :

    pass

    ##


    ##

    ##

##

##
