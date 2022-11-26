from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

#split by ,
columns = 'tradeName,genericName,IssueDate,ExpiryDate,Manufacture_CountryEn,IsLocalManufacturer,Jurisdiction_en,deviceType_en,Classification_en,accessories'.split(',')

class MedicalDevices(MRJob):
 def steps(self):
     return[
        MRStep(mapper=self.mapper_get_ratings,
              reducer=self.reducer_count_ratings)
        ]
#Mapper function 
 def mapper_get_ratings(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           Device = diction['Manufacture_CountryEn']
           #outputing as key value pairs
           yield Device, 1
           
#Reducer function
 def reducer_count_ratings(self, key, values):
       yield key, sum(values)


if __name__ == "__main__":
    MedicalDevices.run()
