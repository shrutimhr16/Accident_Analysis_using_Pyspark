from src.utils import read_csv , write_output , get_spark
from pyspark.sql.functions import col,dense_rank,rank 
from pyspark.sql import Window
from pyspark.sql.functions import row_number,rank


class AccidentAnalysis:
    def __init__(self,spark,config):
        input_paths = config.get("INPUT_FILE_NAME")
        self.charges_df = read_csv(spark, input_paths.get("Charges"))
        self.damages_df = read_csv(spark, input_paths.get("Damages"))
        self.endorse_df = read_csv(spark, input_paths.get("Endorse"))
        self.person_df = read_csv(spark, input_paths.get("Primary_Person") )
        self.units_df = read_csv(spark, input_paths.get("Units"))
        self.restrict_df = read_csv(spark, input_paths.get("Restrict"))

    
    def count_male_accidents(self,output_path, output_format):
        """count number of crashes (accidents) in which number 
        of males killed are greater than 2
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - int: The count of crashes in which number of males killed are greater than 2
        """
        df=(self.person_df.filter(
            self.person_df.PRSN_GNDR_ID=='MALE').groupby('CRASH_ID').count()).where(col('count')>2)
        write_output(df, output_path, output_format)
        return df.count()

    

    def count_two_wheeler_accidents(self,output_path, output_format):
        """number of two wheelers are booked for crashes? 
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - int: The count of two wheeler crashes
        """
        df = self.units_df.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        write_output(df, output_path, output_format)
        return df.count()  
    
    def display_top_5_car_brands_with_faulty_airbags_accidents(self,output_path, output_format):
        """Determine the Top 5 Vehicle Makes of the cars present in 
        the crashes in which driver died and Airbags did not deploy 
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - list: top 5 vehical brands
        """
        df = self.person_df.join(self.units_df,on='CRASH_ID',how='inner').filter(
    (col('PRSN_AIRBAG_ID')=='NOT DEPLOYED') & 
    (col("PRSN_INJRY_SEV_ID") == "KILLED") & 
    (col("VEH_MAKE_ID") != "NA")).groupby("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)
        write_output(df, output_path, output_format)
        return [row[0] for row in df.collect()]


    def count_drivers_with_hitandrun_and_valid_license(self,output_path, output_format):
        """Determine number of Vehicles with driver having 
        valid licences involved in hit and run? 
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - int : count of drivers
        """
        df = (self.units_df.join(self.person_df,
                on=["CRASH_ID"],how="inner").filter((col("VEH_HNR_FL") == "Y")& (col("DRVR_LIC_TYPE_ID").isin(
                        ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])))).drop(self.person_df.DEATH_CNT,self.person_df.INCAP_INJRY_CNT,self.person_df.NONINCAP_INJRY_CNT,self.person_df.POSS_INJRY_CNT,self.person_df.NON_INJRY_CNT,self.person_df.UNKN_INJRY_CNT,self.person_df.TOT_INJRY_CNT,self.person_df.UNIT_NBR)
        write_output(df, output_path, output_format)
        return df.count()
    

    def state_with_max_accidents_without_females(self,output_path, output_format):
        """#Which state has highest number of accidents in
          which females are not involved? 
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - str : name of state
        """
        df = (self.person_df.filter(self.person_df.PRSN_GNDR_ID != "FEMALE")
            .groupby("DRVR_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
        )
        
        write_output(df, output_path, output_format)
        return df.first()[0]       

    def display_top_3_to_5_veh_make_id_involved_in_injuries(self,output_path, output_format):
        """Which are the Top 3rd to 5th VEH_MAKE_IDs that 
        contribute to a largest number of injuries including death 
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - list : name of vehicle make id
        """
        df = (self.units_df.filter(self.units_df.VEH_MAKE_ID != "NA")
            .withColumn("TOT_CASUALTIES_CNT", self.units_df['TOT_INJRY_CNT'] + self.units_df['DEATH_CNT'])
            .groupby("VEH_MAKE_ID")
            .sum("TOT_CASUALTIES_CNT")
            .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOTAL_CASUALTIES_COUNT")
            .orderBy(col("TOTAL_CASUALTIES_COUNT").desc()))
        
        output_df = df.limit(5).subtract(df.limit(2))
        write_output(output_df, output_path, output_format)
        return [row[0] for row in output_df.collect()]     


    def display_top_ethnic_group_with_body_style(self,output_path, output_format):
        """For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - dataframe : top ethnic user group with each unique body style  
        """

        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = self.units_df.join(self.count_drivers_with_hitandrun_and_valid_licenseperson_df, on=["CRASH_ID"], how="inner").filter(
                ~self.units_df.VEH_BODY_STYL_ID.isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])).filter(~self.person_df.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])).groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count().withColumn("rank", rank().over(w)).filter(col("rank") == 1).drop('rank','count')
    
        write_output(df, output_path, output_format)
        return df


    def display_top_ethnic_group_with_body_style(self,output_path, output_format):
        """ Among the crashed cars, what are the Top 5 Zip Codes 
        with highest number crashes with alcohols as the contributing 
        factor to a crash (Use Driver Zip Code)
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - dataframe : top ethnic user group with each unique body style  
        """

        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = self.units_df.join(self.person_df, on=["CRASH_ID"], how="inner").filter(
                ~self.units_df.VEH_BODY_STYL_ID.isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])).filter(~self.person_df.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])).groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count().withColumn("rank", rank().over(w)).filter(col("rank") == 1).drop('rank','count')
    
        write_output(df, output_path, output_format)
        return df

    def count_crashid_with_no_damage_with_insurance(self,output_path, output_format):
        """ Count of Distinct Crash IDs where No Damaged Property was 
        observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - dataframe : top ethnic user group with each unique body style  
        """

        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = self.units_df.join(self.units_df, on=["CRASH_ID"], how="inner").filter(
                ~self.units_df.VEH_BODY_STYL_ID.isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])).filter(~self.person_df.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])).groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count().withColumn("rank", rank().over(w)).filter(col("rank") == 1).drop('rank','count')
    
        write_output(df, output_path, output_format)
        return df

    def get_top_5_zip_codes_had_crashes_due_to_alcohol( self, output_path, output_format):
        """
        Finds the top 5 Zip Codes with the highest number of crashes where alcohol is a contributing factor.
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - List[str]: The top 5 Zip Codes with the highest number of alcohol-related crashes.

        """
        df = (
            self.units_df.join(self.person_df, on=["CRASH_ID"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")
                | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def display_crash_ids_with_no_damage(self, output_path, output_format):
        """
        Counts distinct Crash IDs where no damaged property was observed, the damage level (VEH_DMAG_SCL) is above 4,
        and the car has insurance.
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - List[str]: The list of distinct Crash IDs meeting the specified criteria.
        """
        df = (
            self.damages_df.join(self.units_df, on=["CRASH_ID"], how="inner")
            .filter(
                (
                    (self.units_df.VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                    & (
                        ~self.units_df.VEH_DMAG_SCL_1_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
                | (
                    (self.units_df.VEH_DMAG_SCL_2_ID > "DAMAGED 4")
                    & (
                        ~self.units_df.VEH_DMAG_SCL_2_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
            )
            .filter(self.damages_df.DAMAGED_PROPERTY == "NONE")
            .filter(self.units_df.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        )
        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def display_top_5_vehicle_brand(self, output_path, output_format):
        """
        Determines the top 5 Vehicle Makes/Brands where drivers are charged with speeding-related offences,
        have licensed drivers, use the top 10 used vehicle colours, and have cars licensed with the top 25 states
        with the highest number of offences. Parameters: - output_format (str): The file format for writing the
        output. - output_path (str): The file path for the output file. Returns: - List[str]: The list of top 5
        Vehicle Makes/Brands meeting the specified criteria.
        """
        top_25_state_list = [
            row[0]
            for row in self.units_df.filter(
                col("VEH_LIC_STATE_ID").cast("int").isNull()
            )
            .groupby("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        top_10_used_vehicle_colors = [
            row[0]
            for row in self.units_df.filter(self.units_df.VEH_COLOR_ID != "NA")
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]

        df = (
            self.charges_df.join(self.person_df, on=["CRASH_ID"], how="inner")
            .join(self.units_df, on=["CRASH_ID"], how="inner")
            .filter(self.charges_df.CHARGE.contains("SPEED"))
            .filter(
                self.person_df.DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(self.units_df.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.units_df.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )

        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

