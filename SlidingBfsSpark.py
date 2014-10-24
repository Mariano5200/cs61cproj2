from pyspark import SparkContext
import Sliding, argparse

def bfs_map(arg):
    """ YOUR CODE HERE """
    if arg[1] == level:
        children =  Sliding.children(WIDTH, HEIGHT, arg[0])
        toreturn = [arg]
        for position in children:
            toreturn.append((position, level + 1))
        return toreturn
    else:
        return [arg]
    # return Sliding.children(WIDTH, HEIGHT, value)

def bfs_reduce(arg1, arg2):
    """ YOUR CODE HERE """
    return min(arg1, arg2)
    # return value1 + value2

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT=height
    WIDTH=width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job
    #THIS MEANS THAT MAPREDUCE FROM LEVEL TO NEXT LEVEL

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)


    """ YOUR MAP REDUCE PROCESSING CODE HERE """
    rdd = [(sol, 0)]
    prevcount = 0
    c = 1
    rdd = sc.parallelize(rdd)
    k = 0

    while c != prevcount:
        if k == 10:
            rdd.partitionBy(8)
            k = 0
        rdd = rdd.flatMap(bfs_map) \
                .reduceByKey(bfs_reduce, numPartitions=16)
        prevcount = c
        c = rdd.count()
        level += 1
        k += 1

    finalsolution = rdd.collect()

    ##SET METHOD: THIS SHIT WORKS
    # isdone = False
    # level_to_pos = {}
    # pos_to_level = {}
    # visited = [sol]
    # visited = set(visited)
    # currBoard = []
    # currBoard.append(sol)
    # rdd = sc.parallelize(currBoard)
    # level = 0
    # level_to_pos[level] = [sol]
    # level += 1

    # while isdone == False:
    #     rdd = rdd.map(bfs_map) \
    #             .reduce(bfs_reduce)
    #     currlevelset = set(rdd) #gets rid of duplicates
    #     currlevelset = currlevelset.difference(visited) #gets rid of positions visited in previous levels
    #     if not currlevelset:
    #         isdone = True
    #     level_to_pos[level] = list(currlevelset) #adds the remaining positions to current level
    #     visited = visited.union(currlevelset)
    #     rdd = sc.parallelize(list(currlevelset))
    #     level += 1

    """ YOUR OUTPUT CODE HERE """
    #SET METHOD
    # sc.stop()
    # for i in level_to_pos:
    #     output(str(level_to_pos[i]))

    #kv method
    sc.stop()
    for positiontuple in finalsolution:
        output(str(positiontuple[1]) + " " + str(positiontuple[0]))


""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
