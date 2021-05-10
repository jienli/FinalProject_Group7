# Large Scale Data Processing: Final Project
### By Group 7 - Mokun Li, Xinyu Yao, Jien Li


## Methods
Out group is using two-step process to find the matchings in a graph. The first step is the Israeli-Itai algorithm, as discussed in the course, to find a maximal set of matchings in the graph. The second step is a augmentation step that specifically increase the matching size by 1 for every 3-edge augmenting path found.

### Israeli-Itai Algorithm
The Israeli-Itai Algorithm will find a maximal matching that will have at least 1/2 of number of matching as the maximum matching of the graph. The algorithm is identical as the slide describes and will not be restated here.
The implementation is composed of 3 aggrefateMesage

### 3-Edge Augmentation



## Graph matching Results

|           File name           |        Number of edges       |        Matchings Found       |        Passed Verifier?       |     Time (sec)            |
| ------------------------------| ---------------------------- | ---------------------------- | ---------------------------- |----------------------------|
| com-orkut.ungraph.csv         | 117185083                    | 1408738                      | Y                            |            |
| twitter_original_edges.csv    | 63555749                     | 92583                     |Y                                |            |
| soc-LiveJournal1.csv          | 42851237                     | 1780692                    | Y                              |            |
| soc-pokec-relationships.csv   | 22301964                     | 664398                    | Y                               |            |
| musae_ENGB_edges.csv          | 35324                        | 2452                    | Y                                 |            |
| log_normal_100.csv            | 2671                         | 47                     | Y                                  |27          |










