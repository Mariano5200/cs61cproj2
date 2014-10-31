from pyspark import SparkContext
import Sliding, argparse

def bfs_map(arg):
    """ YOUR CODE HERE """
    if arg[1] == level:
        children =  Sliding.children(WIDTH, HEIGHT, hash_to_board(arg[0]))
        toreturn = [arg]
        for position in children:
            toreturn.append((board_to_hash(position), level + 1))
        return toreturn
    else:
        return [arg]

def bfs_reduce(arg1, arg2):
    """ YOUR CODE HERE """
    return min(arg1, arg2)

def solve_sliding_puzzle(master, output, height, width, slaves):
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
            rdd.partitionBy(PARTITION_COUNT)
            k = 0
        rdd = rdd.flatMap(bfs_map) \
                .reduceByKey(bfs_reduce, numPartitions=16)
        prevcount = c
        c = rdd.count()
        level += 1
        k += 1

    #finalsolution = rdd.collect()

    sc.stop()
    rdd.coalesce(slaves).saveAsTextFile(output)
    # for positiontuple in finalsolution:
    #     output(str(positiontuple[1]) + " " + str(positiontuple[0]))

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
    parser.add_argument("-S", "--slaves", type=int, default=6,
            help="number of slaves executing the job")
    args = parser.parse_args()

    global PARTITION_COUNT
    PARTITION_COUNT = args.slaves * 16

    # call the puzzle solver
    solve_puzzle(args.master, args.output, args.height, args.width, args.slaves)

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
