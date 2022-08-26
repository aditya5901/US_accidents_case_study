from utils.helper import create_spark_session, json_parser
from pyspark.sql.functions import col, row_number, count, rank, translate
from pyspark.sql.window import Window


class CarCrash:
    def __init__(self, spark1):
        self.spark = spark1

    def male_deaths(self, primary_prsn_file):
        """
        :param primary_prsn_file: Path of Primary prsn file
        :return: count of crashes in which number of persons killed are male
        """
        count3 = self.spark.read.option("header", "true") \
            .csv("{}".format(primary_prsn_file)) \
            .where((col("PRSN_GNDR_ID") == 'MALE') & (col("DEATH_CNT") > 0)) \
            .select("CRASH_ID").distinct().count()
        return count3

    def two_wheeler_involved(self, units_use):
        """
        :param units_use: path of units file
        :return: count of two wheelers that are booked for crashes
        """
        count2 = self.spark.read.option("header", "true") \
            .csv("{}".format(units_use)) \
            .where(col("VEH_BODY_STYL_ID") == 'MOTORCYCLE') \
            .select("VIN").distinct().count()
        return count2

    def state_with_highest_females_involved(self, primary_prsn_file):
        """
        :param primary_prsn_file: Path of Primary prsn file
        :return: Returns the dataframe with state has highest number of accidents in which females are involved
        """
        df = self.spark.read.option("header", "true") \
            .csv("{}".format(primary_prsn_file)) \
            .where(col("PRSN_GNDR_ID") == 'FEMALE')
        df = df.groupBy("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(1)
        highest = df.select("DRVR_LIC_STATE_ID")
        return highest

    def vehicle_maker_with_large_no_injuries(self, units_use):
        """
        :param units_use: path of units file
        :return: returns a dataframe Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        df = self.spark.read.option("header", "true") \
            .csv("{}".format(units_use)) \
            .where("TOT_INJRY_CNT != 0 and DEATH_CNT != 0")
        df_grouped = df.groupBy("VEH_MAKE_ID").count()
        df_windowed = df_grouped.withColumn("rk", row_number().over(Window.orderBy(col("count").desc())))
        df_filtered = df_windowed.where(col("rk").between(5, 15))
        return df_filtered

    def top_ethnic_group_for_body_style(self, units_use, primary_prsn_file):
        """
        :param units_use: Path of units file
        :param primary_prsn_file: Path of Primary prsn file
        :return: A dataframe of top ethnic user group of each unique body style
        """
        df_u = self.spark.read.option("header", "true") \
            .csv("{}".format(units_use)).select("CRASH_ID", "UNIT_NBR", "VEH_BODY_STYL_ID")
        df_p = self.spark.read.option("header", "true") \
            .csv("{}".format(primary_prsn_file)).select("CRASH_ID", "UNIT_NBR", "PRSN_ETHNICITY_ID")
        df_join = df_p.join(df_u, [df_p.CRASH_ID == df_u.CRASH_ID
            , df_p.UNIT_NBR == df_u.UNIT_NBR])
        df_count = df_join.withColumn("rk", count("PRSN_ETHNICITY_ID") \
                                      .over(Window.partitionBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")))
        df_max = df_count.groupBy("VEH_BODY_STYL_ID").max("rk")
        df_dropped = df_count.drop("CRASH_ID").distinct()
        df_out = df_max.join(df_dropped, "VEH_BODY_STYL_ID") \
            .where(col("max(rk)") == df_dropped.rk) \
            .drop("max(rk)", "rk")
        return df_out

    def highest_alcoholic_zip_codes(self, primary_prsn_file, units_use):
        """
        :param units_use: Path of units file
        :param primary_prsn_file: Path of Primary prsn file
        :return: returns a dataframe of Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        """
        df = self.spark.read.option("header", "true") \
            .csv("{}".format(primary_prsn_file)).select("CRASH_ID", "UNIT_NBR", "PRSN_ALC_RSLT_ID", "DRVR_ZIP")
        df_alcoholic = df.where("PRSN_ALC_RSLT_ID = 'Positive'")
        df_cars = self.spark.read.option("header", "true") \
            .csv("{}".format(units_use)).select("CRASH_ID", "UNIT_NBR", "VEH_BODY_STYL_ID") \
            .where("VEH_BODY_STYL_ID='PASSENGER CAR, 4-DOOR'")
        df_count = df_alcoholic.join(df_cars, [df_alcoholic.CRASH_ID == df_cars.CRASH_ID
            , df_alcoholic.UNIT_NBR == df_cars.UNIT_NBR]).select(df_alcoholic["*"])
        df_grouped = df_count.groupBy("DRVR_ZIP").count()
        df_ranked = df_grouped.withColumn("rk", rank().over(Window.orderBy(col("count").desc())))
        return df_ranked.where("rk<=5")

    def damage_and_insurance(self, units_use, damages_use):
        """
        :param units_use: path of file
        :param damages_use: path of file
        :return: returns the count
        """
        df = self.spark.read.option("header", "true") \
            .csv("{}".format(units_use))
        df_damage = self.spark.read.option("header", "true") \
            .csv("{}".format(damages_use))
        df_no_damage = df.join(df_damage, "CRASH_ID", "left_anti").select("CRASH_ID", "FIN_RESP_PROOF_ID")
        df_above_4 = df.withColumn('value', translate(col("VEH_DMAG_SCL_1_ID"),
                                                      'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXY ', '').cast(
            "int"))
        df_above_4 = df_above_4.where("value>4").select("CRASH_ID", "FIN_RESP_PROOF_ID")
        df_unioned = df_no_damage.union(df_above_4)
        count1 = df_unioned.where("FIN_RESP_PROOF_ID=1").distinct().count()
        return count1

    def analysis_8(self, units_use, primary_prsn_file, charges):
        """
        :param units_use: Path of units file
        :param primary_prsn_file: Path of Primary prsn file
        :param charges: charges files
        :return: Return a dataframe as per Analysis 8
        """
        df_prsn = self.spark.read.option("header", "true") \
            .csv("{}".format(primary_prsn_file))
        df_unit = self.spark.read.option("header", "true") \
            .csv("{}".format(units_use))
        df_charges = self.spark.read.option("header", "true") \
            .csv("{}".format(charges))

        top_10_color = df_unit.groupBy("VEH_COLOR_ID").count()\
            .withColumn("rk", rank().over(Window.orderBy(col("count").desc()))).where("rk<=10")
        top_25_states = df_unit.groupBy("VEH_LIC_STATE_ID").count() \
            .withColumn("rk", rank().over(Window.orderBy(col("count").desc()))).where("rk<=25")

        df_joined1 = df_unit.join(df_charges, [df_unit.CRASH_ID == df_charges.CRASH_ID
            , df_unit.UNIT_NBR == df_charges.UNIT_NBR]) \
            .where("CHARGE like '%SPEED'").select(df_unit["*"])
        df_joined2 = df_joined1.join(df_prsn, [df_joined1.CRASH_ID == df_prsn.CRASH_ID
            , df_joined1.UNIT_NBR == df_prsn.UNIT_NBR])\
            .where("DRVR_LIC_TYPE_ID = 'DRIVER LICENSE'").select(df_joined1["*"])
        top_5_manufacturer = df_joined2.join(top_25_states, "VEH_LIC_STATE_ID")\
            .join(top_10_color, "VEH_COLOR_ID")\
            .groupBy("VEH_MAKE_ID").count()\
            .withColumn("rk", rank().over(Window.orderBy(col("count").desc()))).where("rk<=5")
        return top_5_manufacturer


spark = create_spark_session()
crash = CarCrash(spark)
data = json_parser()
male_deaths = crash.male_deaths(data["Primary_Person_use"])
print(male_deaths)
two_wheeler_involved = crash.two_wheeler_involved(data["Units_use"])
print(two_wheeler_involved)
state_with_highest_females_involved = crash.state_with_highest_females_involved(data["Primary_Person_use"])
state_with_highest_females_involved.show(20, 0)
vehicle_maker_with_large_no_injuries = crash.vehicle_maker_with_large_no_injuries(data["Units_use"])
vehicle_maker_with_large_no_injuries.show(20, 0)
top_ethnic_group_for_body_style = crash.top_ethnic_group_for_body_style(data["Units_use"], data["Primary_Person_use"])
top_ethnic_group_for_body_style.show(20, 0)
highest_alcoholic_zip_codes = crash.highest_alcoholic_zip_codes(data["Primary_Person_use"], data["Units_use"])
highest_alcoholic_zip_codes.show(20, 0)
print(crash.damage_and_insurance(data["Units_use"], data["Damages_use"]))
analysis_8 = crash.analysis_8(data["Units_use"], data["Primary_Person_use"], data["Charges_use"])
analysis_8.show(20, 0)
