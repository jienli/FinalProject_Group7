# Large Scale Data Processing: Final Project
## Graph matching
For the final project, you are provided 6 CSV files, each containing an undirected graph, which can be found [here](https://drive.google.com/file/d/1khb-PXodUl82htpyWLMGGNrx-IzC55w8/view?usp=sharing). The files are as follows: 


|           File name           |        Number of edges |        Matchings Found       |        Passed Verifier?       |     Time             |  Iterations  |  Augmented Iterations |
| ------------------------------| -------------------| ---------------------------- | ---------------------------- |----------------------------| -----------| ----------|
| com-orkut.ungraph.csv         | 117185083                    | 1408738                      | Y                            | 2h 19m 54s on a 2 * 16 N1 core CPU in GCP           |  20 | 2 |
| twitter_original_edges.csv    | 63555749                     | 92583                     |Y                                |  1h 53m 12s on a 2 * 16 N1 core CPU in GCP          |  22  | 3 |
| soc-LiveJournal1.csv          | 42851237                     | 1780692                    | Y                              |  1h 12m 32s on a 2 * 16 N1 core CPU in GCP         | 20   | 3|
| soc-pokec-relationships.csv   | 22301964                     | 664398                    | Y                               |   s on a 2 * 16 N1 core CPU in GCP         |  23  | 4|
| musae_ENGB_edges.csv          | 35324                        | 2452                    | Y                                 | 38s on local           | 13| 23|
| log_normal_100.csv            | 2671                         | 47                     | Y                                  | 27s on local          | 8  | 1 |


## Deliverables

* A project report that includes the following:
  * A table containing the size of the matching you obtained for each test case. The sizes must correspond to the matchings in your output files.
  * An estimate of the amount of computation used for each test case. For example, "the program runs for 15 minutes on a 2x4 N1 core CPU in GCP." If you happen to be executing mulitple algorithms on a test case, report the total running time.
  * Description(s) of your approach(es) for obtaining the matchings. It is possible to use different approaches for different cases. Please describe each of them as well as your general strategy if you were to receive a new test case.
  * Discussion about the advantages of your algorithm(s). For example, does it guarantee a constraint on the number of shuffling rounds (say `O(log log n)` rounds)? Does it give you an approximation guarantee on the quality of the matching? If your algorithm has such a guarantee, please provide proofs or scholarly references as to why they hold in your report.
  * Israeli-Itai Algorithm
  ** O(log⁡ n) rounds 
  ** Pr(no neighbor proposed to v)≤(1−1/𝑑(𝑣) )^(𝑑(𝑣)/3)≤𝑒^(−1/3)
  ** Pr(v got proposed) ≥1−𝑒^(−1/3)
  ** Pr(v got deleted) ≥(1−𝑒^(−1/3))/4




