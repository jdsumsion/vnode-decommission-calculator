Explanation
===

If you use Cassandra with vnodes, and you want to decommission a node, you
might be asking yourself, "Does it matter which node I decommission?"

It does matter, depending on how your nodes were bootstrapped, and how vnodes
ended up being allocated.

Maintenance
===

Sorry the code isn't better commented.  I only had enough time to get this
written and manually verified.  Feel free to submit documentation.

I'm also only an intermediate Rubyist, so there are probably a lot of cleanups
you could find here.

If you love Python, feel free to submit a Python version of this.

Sample
===

The following output was produced with DSE 5.1.7, Cassandra 3.11.1, Ruby 2.5.3p105:
```
$ ruby vnode-decommission-calculator.rb ring-sample.out 
calculating...

nodes to remove from each rack: 2
node count: 51
rack count: 7
rack descr:
- dc2:1a: 6
- dc1:1a: 8
- dc1:1b: 8
- dc1:1c: 8
- dc3:2c: 7
- dc3:2a: 7
- dc3:2b: 7
token count: 5856

decommission plan:
=> dc2:1a: first 10.2.1.6, then 10.2.1.5 (next best: first 10.2.1.4, then 10.2.1.2)
=> dc1:1a: first 10.1.1.4, then 10.1.1.7 (next best: first 10.1.1.8, then 10.1.1.3)
=> dc1:1b: first 10.1.2.4, then 10.1.2.7 (next best: first 10.1.2.8, then 10.1.2.2)
=> dc1:1c: first 10.1.3.4, then 10.1.3.8 (next best: first 10.1.3.3, then 10.1.3.7)
=> dc3:2c: first 10.3.3.7, then 10.3.3.6 (next best: first 10.3.3.5, then 10.3.3.2)
=> dc3:2a: first 10.3.1.3, then 10.3.1.7 (next best: first 10.3.1.5, then 10.3.1.1)
=> dc3:2b: first 10.3.2.2, then 10.3.2.6 (next best: first 10.3.2.4, then 10.3.2.3)
```
