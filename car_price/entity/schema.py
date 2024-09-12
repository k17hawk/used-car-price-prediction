from typing import List
from pyspark.sql.types import (TimestampType, 
            StringType, FloatType, StructType, StructField)
from car_price.exception import CarPriceException
from pyspark.sql import DataFrame
import os, sys
from typing import Dict

class CarPriceDataSchema:

    def __init__(self) -> None:
        self.col_id = "id"
        self.col_col_model_year = "col_model_year"
        self.col_milage = 'milage'
        self.price = 'price'
        self.col_brand = 'brand'
        self.col_model = 'model'
        self.col_fuel_type = 'fuel_type'
        self.col_engine = 'engine'
        self.col_transmission = 'transmission'
        self.col_ext_col = 'ext_col'
        self.col_int_col = 'int_col'
        self.col_accident = 'accident'
        self.col_clean_title = 'clean_title'
        
    @property
    def dataframe_schema(self)  -> StructType:
        try:
            schema = StructType([
                StructField(self.col_id,StringType()),
                StructField(self.col_col_model_year,StringType()),
                StructField(self.col_milage,StringType()),
                StructField(self.price,StringType()),
                StructField(self.col_brand,StringType()),
                 StructField(self.col_model,StringType()),
                StructField(self.col_fuel_type,StringType()),
                StructField(self.col_engine,StringType()),
                StructField(self.col_transmission,StringType()),
                StructField(self.col_ext_col,StringType()),
                StructField(self.col_int_col,StringType()),
                StructField(self.col_accident,StringType()),
                StructField(self.col_clean_title,StringType()),

            ])
            return schema
        except Exception as e:
            raise CarPriceException(e,sys) from e
        
    @property
    def target_column(self):
        return self.price
    
    @property
    def target_column(self):
        return self.price

    @property
    def categorical_features(self):
        features = [
            self.col_brand,
            self.col_model,
            self.col_fuel_type,
            self.col_engine,
            self.col_transmission,
            self.col_ext_col,
            self.col_int_col,
            self.col_accident,
            self.col_clean_title
        ]
        return features
    
    @property
    def unwanted_columns(self) -> List[str]:
        features = [
            self.col_id,
        ]
        return features
    

    @property
    def numerical_features(self)-> List[str]:
        features = [
            self.col_col_model_year,
            self.col_milage
        ]
        return features
    
    @property
    def ordinal_encoding(self)-> List[str]:
        features = [
               f"enc_{col}" for col in self.categorical_features
        ]
        return features
    





        
    



