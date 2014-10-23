from pyspark import SparkContext
import Sliding, argparse

def bfs_map(value):
    """ YOUR CODE HERE """
    return Sliding.children(WIDTH, HEIGHT, value)

def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    return value1 + value2

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
    prevrdd = rdd
    isdone = False	
    pos_to_level = {}

    def flatmap(arg):
        if arg[1] == level:
            children =  Sliding.children(WIDTH, HEIGHT, arg[0])
            toreturn = [arg]
            for position in children:
                toreturn.append((position, level + 1))
            return [toreturn]
        else:
            return arg

    def reduce(arg1, arg2):
        return arg1 + arg2

    while isdone == False:
        prevrdd = rdd
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        print(rdd)
        rdd = sc.parallelize(rdd)
        rdd = rdd.flatMap(flatmap) \
                .reduce(reduce)
        if (rdd == prevrdd):
            isdone = True
        level += 1

    finalsolution = rdd.collect()
    for positiontuple in finalsolution:
        pos_to_level[positiontuple[0]] = positiontuple[1]

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
    #     if currlevelset.issubset(visited):
    #         isdone = True
    #     currlevelset = currlevelset.difference(visited) #gets rid of positions visited in previous levels
    #     if not currlevelset:
    #         isdone = True
    #     level_to_pos[level] = list(currlevelset) #adds the remaining positions to current level
    #     visited = visited.union(currlevelset)
    #     rdd = sc.parallelize(list(currlevelset))
    #     level += 1

    """ YOUR OUTPUT CODE HERE """

    sc.stop()
    for i in pos_to_level:
        output(pos_to_level[i] + " "+ str(i))


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
