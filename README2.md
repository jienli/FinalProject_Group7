# Large Scale Data Processing: Final Project
### By Group 7 - Mokun Li, Xinyu Yao, Jien Li


# Methods
Out group is using two-step process to find the matchings in a graph. The first step is the Israeli-Itai algorithm, as discussed in the course, to find a maximal set of matchings in the graph. The second step is a augmentation step that specifically increase the matching size by 1 for every 3-edge augmenting path found.

## Israeli-Itai Algorithm
The Israeli-Itai Algorithm will find a maximal matching that will have at least 1/2 of number of matching as the maximum matching of the graph. The algorithm is identical as the slide describes and will not be restated here.
The implementation is composed of 3 aggrefateMesages: Proposal, Acceptance, and Deactivation. Choosing a random neighbor to propose is achieved by each neighbor sending a random number and choosing the largest number as the destination of the proposal. Acceptance of the proposal is just choosing the proposal with the largest id nubmer, since a strictly random selection is not required. The deactivation is simply deactivating vertices based on the status of the edges. (The newly matched edges will be marked through a `mapTriplets()` call)

Each iteration will match a constant fraction of edges in expectation, and the algorithm will be completed in log(n) rounds.

## 3-Edge Augmentation
An augmenting path can be flipped to increase the total matching size by 1. But finding the augmentitng paths can be complicated and time consuming. Here, we reduce the complexity of the problem by only finding one type of augmenting paths that is composed of 3 edges and 4 vertices. The algorithm is as follows:
 1. Each free vertices choose a random neighboring matched vertex to send a proposal
 2. Each matched vertecies choose a random proposal to accept
 3. If both vertecies of a matched pair have a proposal, flip the augmenting path of 3 edges.
 4. Repeat step 1-3 until the matching size is increasing by less than 2% per round.
Step 1 and 2 garantees that each matched pair and each proposing free vertecies form a 3-edge path that is non-overlapping with other similar 3-edge paths, becasue each proposing free vertecies can only propose to 1 matched vertex and each matched vertices can only accept one proposal. Step 3 joins 2 parts together to a 3-edge path and flip it.

In each iteration, the number of 3-edge augmenting paths found is primarily limited by the probability of 2 free vertecies selecting the correct neighboring matched vertices in step 1. (beause the probability of a matched vertex having multiple proposal in step 2 is actually really small compared to the number of candicates to propose to in step 1) For each potential 3-edge augmenting path, the probability of finding it is roughly equal to the probability of selecting the right neighbors in step 1, and is therefore roughly `(1/d1+1/d2)` with `d1`=degree of free vertex 1 and `d2`=degree of free vertex 2. The sum of `(1/d1+1/d2)` for each potential 3-edge augmenting path is therefore the expected number of new matchings each iteration. Assuming most vertex have roughly equal degrees, and let n be the number of potential 3-edge augmenting paths, the expected number of new matching is therefore `(2n/d)`, which is a constant fraction of the maximum theoretical amount `n`. Therefore, with the algorithm will converge, adn all potential 3-edge augmenting paths will be found.


The implementation is composed of 4 `aggregateMessages()` calls and a `mapTriplets()` call. Random selection is again achieven by sending random numbers and selecting the message with the largest number. The implementatoin technique is very similar to that of the Israeli-Itai Algorithm, and please refer to the original code for more detail.


# Graph matching Results

|           File name           |        Number of edges |        Matchings Found       |        Passed Verifier?       |     Time (sec)            |
| ------------------------------| -------------------| ---------------------------- | ---------------------------- |----------------------------|
| com-orkut.ungraph.csv         | 117185083                    | 1408738                      | Y                            |            |
| twitter_original_edges.csv    | 63555749                     | 92583                     |Y                                |            |
| soc-LiveJournal1.csv          | 42851237                     | 1780692                    | Y                              |            |
| soc-pokec-relationships.csv   | 22301964                     | 664398                    | Y                               |            |
| musae_ENGB_edges.csv          | 35324                        | 2452                    | Y                                 | 38           |
| log_normal_100.csv            | 2671                         | 47                     | Y                                  | 27          |







