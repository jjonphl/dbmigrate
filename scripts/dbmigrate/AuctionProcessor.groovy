package dbmigrate

import groovy.sql.Sql

class AuctionProcessor {

    private String auctionDate
    private String isin
    private String maturityDate
    private String issueDate
    private String reIssueDate
    private String couponRate
    private String convYear
    private String maturityPeriod
    private String wtdAveRate
    private String highestRate
    private String lowestRate
    private String wtdAveRatePrice
    private String highestRatePrice
    private String lowestRatePrice

    private Integer instrumentId

    private Sql sql

    def AuctionProcessor(ArrayList details, Sql sql) {
        this.sql = sql

        this.auctionDate = details.get(0)
        this.isin = details.get(1)
        this.maturityDate = details.get(2)
        this.issueDate = details.get(3)
        this.reIssueDate = details.get(4) == "-" ? null : details.get(4)
        this.couponRate = details.get(5) == "-" ? null : details.get(5)
        this.convYear = details.get(6) == "-" ? null : details.get(6)
        this.maturityPeriod = details.get(7) == "-" ? null : details.get(7)
        this.wtdAveRate = details.get(8)
        this.highestRate = details.get(9)
        this.lowestRate = details.get(10)
        this.wtdAveRatePrice = details.get(11)
        this.highestRatePrice = details.get(12)
        this.lowestRatePrice = details.get(13)

        this.instrumentId = getInstrument();
    }

    def process(count){
        print "insertAuction..."
        insertAuction(count)
        println "success"
        print "insertIssueAuction..."
        insertIssueAuction(count)
        println "success"
        print "insertIssueAuctionDetail..."
        insertIssueAuctionDetail(count)
        println "success"
        print "insertIssueSummary..."
        insertIssueSummary(count)
        println "success"
        println count
    }

    def insertAuction(count){
        def id = sql.firstRow('select AuctionId from Auction where IssuerId=? and AuctionDate=?', [2, auctionDate])
        if (!id) {
            def stmt = "insert into `Auction` (`IssuerId`, `AuctionDate`, `StartTime`, `CutOffTime`, `Status`, `CreatedBy`, `CreatedDate`, `UpdatedBy`, `UpdatedDate`) values ('2',${auctionDate},'11:00:00','13:00:00','3','btr',now(),'btr',now())"
            //println stmt
            id = sql.execute(stmt)[0][0]
        }
        return id
    }

    def insertIssueAuction(count){
        def stmt = "insert into `IssueAuction` (`IssueId`, `IssuerId`, `IssueIsin`, `IssueDate`, `MaturityDate`, `CouponRate`, `ConversionYear`, `TaxCodeId`, `Remarks`, `CreatedBy`, `CreatedDate`, `UpdatedBy`, `UpdatedDate`) values (${count},2,${isin},${issueDate},${maturityDate},${couponRate},${convYear},1,remarks,'btr',now(),'btr',now())"
        //println stmt
        sql.execute stmt
    }

    def insertIssueAuctionDetail(count){
        def stmt = "insert into `IssueAuctionDetail` (`IssueDetailId`, `IssueId`, `IssuerId`, `IssueIsin`, `ReissueDate`, `AuctionType`, `Status`, `CurrencyId`,  `AmountOffer`, `AmountAwarded`, `CompAmountOffer`, `NonCompAmountOffer`, `MaxCompVolume`, `MaxNonCompVolume`, `SecondaryRate`, `MaxBidRate`, `PaymentMode`, `CashPercent`, `PlausibilityRate`, `UpperRate`, `LowerRate`, `TaxCodeId`, `InstrumentId`, `AuctionId`, `MaturityPeriod`, `Remarks`, `Settled`, `CreatedBy`, `CreatedDate`, `UpdatedBy`, `UpdatedDate`, `ECSFileSequenceNo`) values (${count},${count},2,${isin},${reIssueDate},1,4,1,1000000000,1000000000,600000000,400000000,null,null,null,null,1,100.00,${wtdAveRate},50,50,1,${instrumentId},${count},${maturityPeriod},null,0,'btr',now(),'btr',now(),null)"
        //println stmt
        sql.execute stmt
    }

    def insertIssueSummary(count){
        def stmt = "insert into `IssueSummary` (`IssueDetailId`, `MaturityPeriod`, `InstrumentId`, `Offering`, `AmountAwarded`, `IssueIsin`, `HighestRate`, `AverageRate`, `LowestRate`, `HighestPrice`, `AveragePrice`, `LowestPrice`, `TenderedCompAmount`, `TenderedNonCompAmount`, `AcceptedCompAmount`, `AcceptedNonCompAmount`, `StopOutAmount`, `StopOutPercent`, `PrevOriginalIssue`, `PrevCouponRate`, `PrevHighestRate`, `PrevAverageRate`, `PrevLowestRate`, `PrevHighestPrice`, `PrevAveragePrice`, `PrevLowestPrice`, `PrevStopOutAmount`, `PrevStopOutPercent`, `Remarks`, `CreatedBy`, `CreatedDate`, `UpdatedBy`, `UpdatedDate`, `StopOutPercentAwarded`, `StopOutPercentAmount`) values (${count},${maturityPeriod},${instrumentId},1000000000,1000000000,${isin},${highestRate},${wtdAveRate},${lowestRate},${highestRatePrice},${wtdAveRatePrice},${lowestRatePrice},600000000,400000000,600000000,400000000,null,null,null,null,null,null,null,null,null,null,null,null,null,'btr',now(),'btr',now(),100.000,null)"
        //println stmt
        sql.execute stmt
    }

    def getInstrument(){
        def code = isin.substring(2,4)
        //println code
        def instrumentType = code.equals("BL") ? "1" : "2"
        def stmt = "select InstrumentId from Instrument i where InstrumentType = ${instrumentType} and MaturityPeriod = ${maturityPeriod} and IssuerId = 2"
        def ret = sql.firstRow(stmt)

        if(ret){
            ret = ret.getProperty("InstrumentId")
        }else{
            def desc = maturityPeriod + (code.equals("BL") ? " Days Treasury Bill" : " Years Treasury Bond")
            int count = sql.firstRow("select count(*) from Instrument").getProperty("count(*)")
            count++
            def keys = sql.executeInsert("insert into Instrument (`InstrumentId`,`IssuerId`,`InstrumentType`,`MaturityPeriod`,`Competitive`,`UpperRate`,`LowerRate`,`CreatedBy`,`CreatedDate`,`UpdatedBy`,`UpdatedDate`,`Description`) values (${count},2,${instrumentType},${maturityPeriod},60,50,50,'btr',now(),'btr',now(),${desc})")
            ret = keys[0][0]
        }

        //println ret
        return ret
    }
}
