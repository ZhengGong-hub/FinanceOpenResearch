from datetime import date, timedelta

from common.utils.logging import get_logger
from common.database.base_database import BaseDatabase
import pandas as pd
logger = get_logger(__name__)


class TaskManagerRepository:
    """Repository for handling task operations with api."""

    def __init__(self, database: BaseDatabase):
        """Initialize repository with database connection.

        Args:
            database: Database instance for data access
        """
        self.database = database

    def test_connection_query(self) -> pd.DataFrame:
        """Test the connection to the database.

        Returns:
            pd.DataFrame: A dataframe with the company ID.
        """
        return self.database.query_all("SELECT * from ciqcompany limit 10;")

    def query_global_market_cap(self, asofdate: str, mktcap_thres: float, country: str = "US", allow_fuzzy: bool = False) -> pd.DataFrame:
        """Query the global market cap that is above the threshold and at a given date.

        we do not really need the fuzzy, as the marketcap is pretty dense over vacations and holidays

        Args:
            asofdate: The date to query the market cap for
            mktcap_thres: The market cap threshold (in million USD)
            country: The country code to filter companies (default: "US")
            allow_fuzzy: If True, look for data within 5 days of asofdate if exact date not available
        Returns:
            pd.DataFrame: A dataframe with the company ID and market cap
        """
        # check asofdate is a str
        if not isinstance(asofdate, str):
            raise ValueError("asofdate must be a string")

        if country == "Global":
            all_countries = True
        else:
            all_countries = False

        # Common SELECT fields and table joins for both scenarios
        query = """
            SELECT 
                ciqmarketcap.companyid,
                ciqmarketcap.marketcap,
                ciqmarketcap.pricingdate,
                round(ciqmarketcap.marketcap / ciqexchangerate.priceclose, 2) as usdmarketcap,
                ciqcompany.companyname,
                ciqtradingitem.tickersymbol,
                ciqcurrency.isocode as currency,
                ciqexchange.exchangesymbol as exchange,
                ciqcountrygeo.isocountry2 as country
            FROM
                ciqmarketcap
            JOIN
                ciqcompany ON ciqmarketcap.companyID = ciqcompany.companyID
            JOIN
                ciqsecurity ON ciqmarketcap.companyID = ciqsecurity.companyID
            JOIN 
                ciqtradingitem on ciqsecurity.securityid = ciqtradingitem.securityid
            JOIN 
                ciqexchangerate on ciqtradingitem.currencyid = ciqexchangerate.currencyid
            JOIN
                ciqcurrency on ciqtradingitem.currencyid = ciqcurrency.currencyid
            JOIN
                ciqexchange on ciqtradingitem.exchangeid = ciqexchange.exchangeid
            JOIN 
                ciqcountrygeo on ciqcompany.countryid = ciqcountrygeo.countryid
            WHERE
        """

        # Date conditions differ based on allow_fuzzy
        if allow_fuzzy:
            query += f"""
                ciqmarketcap.pricingdate BETWEEN DATE('{asofdate}') - INTERVAL '3 days' AND '{asofdate}'
            """
        else:
            query += f"""
                ciqmarketcap.pricingdate = '{asofdate}'
            """

        # add country filter if not all countries
        if all_countries:
            pass
        else:
            query += f"""
                AND 
                    ciqcountrygeo.isocountry2 = '{country}'
            """

        # Common WHERE conditions for both scenarios
        query += f"""
            AND
                ciqexchangerate.pricedate = '{asofdate}'
            AND
                ciqexchangerate.latestsnapflag = 1
            AND
                ciqmarketcap.marketcap / ciqexchangerate.priceclose >= {mktcap_thres}
            AND
                ciqcompany.companytypeid in (4, 5)
            AND 
                ciqsecurity.primaryflag = 1
            AND 
                ciqtradingitem.primaryflag = 1
            ORDER BY
                ciqmarketcap.pricingdate DESC, usdmarketcap DESC
        """

        return self.database.query_all(query)


    def get_hist_miadj_pricing(self,start, end, ls_ids):
        """
        Get a historical price data given a series of company ids, using miadjusted table 
        instead of ciqpeequity table 
        
        Args:
            start (str): '2020-05-05'
            end (str): '2020-06-06'
            ls_ids (list): list of companyid   [24937, ]
            connection (None, optional): Description
        
        Returns:
            sample ouput: 
            companyid  tradingitemid   pricedate      priceclose       priceopen       pricehigh        pricelow                 volume       vwap               divadjclose  divadjfactor
        0       24937        2590360  2020-05-05  74.39000000000  73.76500000000  75.25000000000  73.61500000000  147751200.00000000000  74.637500  73.273994703436000000000  0.9849979124
        1       24937        2590360  2020-05-06  75.15750000000  75.11500000000  75.81000000000  74.71750000000  142333760.00000000000  75.437500  74.029980601203000000000  0.9849979124
        2       24937        2590360  2020-05-07  75.93500000000  75.80500000000  76.29250000000  75.49250000000  115215040.00000000000  75.927500  74.795816478094000000000  0.9849979124
        3       24937        2590360  2020-05-08  77.53250000000  76.41000000000  77.58750000000  76.07250000000  134047960.00000000000  76.947500  76.576081355087250000000  0.9876642873

        """
        startstr = pd.to_datetime(start)
        endstr = pd.to_datetime(end)
        
        query = f"""
        SELECT 
        c.companyid
        ,ti.tradingItemId
        ,ti.currencyid
        ,mi.priceDate
        ,mi.priceClose
        ,mi.priceOpen
        ,mi.priceHigh
        ,mi.priceLow
        ,mi.volume
        ,mi.vwap

        ,(mi.priceClose*COALESCE(daf.divAdjFactor,1)) divAdjClose
        ,COALESCE(daf.divAdjFactor,1) as divAdjFactor

        FROM ciqCompany c
        JOIN ciqSecurity s on s.companyid = c.companyid
        JOIN ciqTradingItem ti on ti.securityId=s.securityId
        JOIN miadjprice mi on mi.tradingItemId=ti.tradingItemId

        left join ciqPriceEquityDivAdjFactor daf on mi.tradingItemId=daf.tradingItemId
        and daf.fromDate<=mi.priceDate --Find dividend adjustment factor on pricing date
        and (daf.toDate is null or daf.toDate>=mi.priceDate)

        WHERE c.companyId in ({', '.join([str(id) for id in ls_ids])})  
        AND s.primaryflag=1 -- empirically makes sense to have these primary flag, lost about 0.03% data
        AND ti.primaryflag=1
        AND mi.priceDate >= '{startstr}'
        AND mi.priceDate <= '{endstr}'
        ORDER BY mi.priceDate asc;
        """
        df = self.database.query_all(query)
        return df


    def get_afl_factor_monthly_period(self, begin, end, factorids, ls_ids):
        sql = f"""
                select 
                dly.factorvalue
                , dly.factorid
                , gvk.objectId
                , dly.asofdate
                , d.securityid
                , gvk.gvkey
                , gvk.iid
                , c.companyid
                from ciqafvaluemonthlyna dly

                join ciqgvkeyiid gvk 
                on gvk.gvkey = dly.gvkey
                and gvk.iid = dly.iid

                JOIN ciqTradingItem d ON gvk.objectId = d.tradingItemId AND d.currencyid = 160 AND d.primaryflag = 1
                JOIN ciqSecurity s ON d.securityid = s.securityid
                JOIN ciqCompany c ON c.companyid = s.companyid

                where dly.factorId in ({', '.join([str(factor_id) for factor_id in factorids])})
                and asOfDate >= '{begin}'
                and asOfDate <= '{end}'
                and c.companyid in ({', '.join([str(id) for id in ls_ids])})
                """

        return self.database.query_all(sql)


    def get_afl_factor_daily_period(self, begin, end, factorids, ls_ids):
        sql = f"""
                select 
                dly.factorvalue
                , dly.factorid
                , gvk.objectId
                , dly.asofdate
                , d.securityid
                , gvk.gvkey
                , gvk.iid
                , c.companyid
                from ciqafvaluedailyna dly

                join ciqgvkeyiid gvk 
                on gvk.gvkey = dly.gvkey
                and gvk.iid = dly.iid

                JOIN ciqTradingItem d ON gvk.objectId = d.tradingItemId AND d.currencyid = 160 AND d.primaryflag = 1
                JOIN ciqSecurity s ON d.securityid = s.securityid
                JOIN ciqCompany c ON c.companyid = s.companyid

                where dly.factorId in ({', '.join([str(factor_id) for factor_id in factorids])})
                and asOfDate >= '{begin}'
                and asOfDate <= '{end}'
                and c.companyid in ({', '.join([str(id) for id in ls_ids])})
                """

        return self.database.query_all(sql)


    def get_estimatediff_ref_co(self, ls_ids, dataitemids, startdate, enddate):
        sql = f"""
            select 
            EP.*
            , ED.dataitemid
            , ED.currencyId
            , ED.dataItemValue
            , ED.asofdate
            , EC.tradingitemid
            , ED.scaleid

            from ciqEstimatePeriod EP
            --- link the core estimate table to data table
            --------------------------------------------------------------
            join ciqEstimateConsensus EC 
            on EC.estimatePeriodId = EP.estimatePeriodId
            join ciqEstimateanalysisdata ED
            on ED.estimateConsensusId = EC.estimateConsensusId
            --------------------------------------------------------------
            where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
            and EP.periodTypeId = 2 -- Quarter 
            and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
            and ED.asofdate >= '{startdate}'
            and ED.asofdate <= '{enddate}'
            order by 4
        """
        
        return self.database.query_all(sql)


    def get_act_q_ref_co(self, ls_ids, dataitemids, fromdate):

        sql = f"""
            select 
            EP.*
            , ED.dataitemid
            , ED.currencyId
            , ED.dataItemValue
            , ED.effectiveDate
            , ED.toDate
            , EC.tradingitemid
            , ED.estimatescaleid

            from ciqEstimatePeriod EP
            --- link the core estimate table to data table
            --------------------------------------------------------------
            join ciqEstimateConsensus EC 
            on EC.estimatePeriodId = EP.estimatePeriodId
            join ciqEstimateNumericData ED
            on ED.estimateConsensusId = EC.estimateConsensusId
            --------------------------------------------------------------
            where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
            and EP.periodTypeId = 2 -- Quarter 
            and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
            and EP.periodenddate > '{fromdate}'
            and ED.toDate > '2030-01-01'

            order by 4
        """
        
        return self.database.query_all(sql)