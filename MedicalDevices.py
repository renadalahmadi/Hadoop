# importing MRJob and MRStep from mrjob package
from mrjob.job import MRJob
from mrjob.job import MRStep

class MedicalDevices(MRJob):
    # Define single mapreduce phase
    def steps(self):
        return [
            MRStep(mapper=self.mapper_device_countries,
            reducer=self.reducer_count_countries)
    ]

    def mapper_device_countries(self, _, line):
         (tradeName, genericName, IssueDate, ExpiryDate, Manufacture_CountryEn,IsLocalManufacturer,Jurisdiction_en, deviceType_en ,Classification_en,accessories) = line.split(',')
        yield Manufacture_CountryEn, 1

    def reducer_count_countries(self, key, values):
        yield key, sum(values)
    
    
if __name__ == '__main__':
    MedicalDevices.run()

