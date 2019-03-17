
** Personal Notes **
- val dataset = Seq((0, "hello"), (1, "world")).toDF("id", "text")
- ds.select("hour", "week_date,density".split(","):_*).show
- val s = "hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-11-29-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-11-22-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-11-15-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-11-08-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-11-01-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-10-25-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-10-18-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-10-11-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-10-04-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-09-27-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-09-20-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-09-13-10,hdfs://clpd743.sldc.sbc.com/opt/app/nrsapp/iqi/vertica/UNI_EMB_COVROAM_U4_2018-09-06-10"

 val f = s.split(",")

val ds = spark.read.load(f:_*)

- import sys.process._

"ls /".!

"hdfs dfs -ls /opt".!

