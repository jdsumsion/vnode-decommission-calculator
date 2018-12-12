WARNING
===

This utility is not ready for prime-time.  I'm not sure if it will ever be a
general-purpose tool.  I've run into issues applying the predictions of this
calculator on real clusters.

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
$ rm ring-sample.out.options; ruby vnode-decommission-calculator.rb ring-sample.out 
calculating...
DC: dc2, Enter node count to decommission (per-rack, default 2): 0
DC: dc1, Enter node count to decommission (per-rack, default 2): 1
DC: dc3, Enter node count to decommission (per-rack, default 2): 2

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
config:
{
  "dc2": {
    "decommission_count": 0
  },
  "dc1": {
    "decommission_count": 1
  },
  "dc3": {
    "decommission_count": 2
  }
}

decommission plan:
=> dc1:1a: 10.1.1.8 [5.30e+18] (next best: 10.1.1.7 [5.61e+18])
=> dc1:1b: 10.1.2.8 [5.30e+18] (next best: 10.1.2.7 [5.61e+18])
=> dc1:1c: 10.1.3.8 [5.30e+18] (next best: 10.1.3.7 [5.61e+18])
=> dc3:2c: first 10.3.3.4, then 10.3.3.3 [7.03e+18] (next best: first 10.3.3.2, then 10.3.3.1 [7.07e+18])
=> dc3:2a: first 10.3.1.1, then 10.3.1.2 [6.96e+18] (next best: first 10.3.1.3, then 10.3.1.5 [6.98e+18])
=> dc3:2b: first 10.3.2.2, then 10.3.2.1 [6.88e+18] (next best: first 10.3.2.4, then 10.3.2.7 [7.14e+18])
```
