CREATE TABLE IF NOT EXISTS my_table
    	(
    	    ReportDt DateTime,
    	    Unit String,
    	    Power UInt16
    	) ENGINE = MergeTree()
    	ORDER BY ReportDt;