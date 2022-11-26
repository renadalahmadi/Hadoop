from mrjob.job import MRJob
from mrjob.job import MRStep


class MedicalDevices(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
            reducer=self.reducer_count_ratings)
    ]
    
def mapper_get_ratings(self, _, line):
    (tradeName, genericName, IssueDate, ExpiryDate, Manufacture_CountryEn,IsLocalManufacturer,Jurisdiction_en, deviceType_en ,Classification_en,accessories) = line.split(',')
    yield rating, 1

    
def reducer_count_ratings(self, key, values):
    yield key, sum(values)
    
    
if __name__ == '__main__':
    MedicalDevices.run()
