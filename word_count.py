def countWords(sc, files, weirdPartition = False):
  output = None
  stpword = ['','ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than']
  def removePunc(word):
    punctuation = {"\\" : "", "?" : "",  
                      "," : "", "\'" : "", '\"' : "", ":" : "", ";" : "",
                      "." : "", "!" : "", "(" : "", ")" : "", "{" : "",
                      "}" : "", "[" : "", "]" : "", "/" : "", "*" : "" }
    res = word.maketrans(punctuation)
    word = word.translate(res)
    return word
  
  for file in files:
    if weirdPartition:
      # DON'T ACTUALLY DO THIS! Just for demo purposes
      lines = sc.textFile(file, minPartitions = len(file)) 
    else:
      lines = sc.textFile(file) 
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(removePunc) \
                  .map(lambda x: (x.lower(), 1)) \
                  .filter(lambda x: x[0] not in stpword) \
                  .reduceByKey(lambda a,b: a + b) 
                  

    if output == None:
      output = (counts).sortBy(lambda x : x[1], ascending = False)
    else:
      output = output.union(counts).sortBy(lambda x : x[1], ascending = False)
  return output



from pyspark.context import SparkContext
sc = SparkContext('local', 'test')
countWords(sc, ["anzac.txt", "bibliography.txt", "bleakhouse.txt","conquest.txt", "geographicaldiscovery.txt","nutall.txt","olddevonshire.txt","scarletplague.txt","tokoshire.txt","traininginfantry.txt"]).saveAsTextFile("output")

