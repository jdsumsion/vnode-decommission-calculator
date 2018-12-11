#!/usr/bin/env ruby

require 'json'

def usage_and_exit
  $stderr.puts "usage: #{File.basename($0)} <nodetool-ring.out>"
  exit 1
end

unless ARGV.size >= 1 and File.exist? ARGV[0]
  usage_and_exit
end

###
# For extra debug output, set DEBUG=true
DEBUG=false

def debug(*args)
  puts *args if DEBUG
end

class Node

  attr_accessor :ip, :dc, :rack, :dc_and_rack, :tokens

  def initialize(ip, dc, rack, tokens=[])
    @ip = ip
    @dc = dc
    @rack = rack
    @dc_and_rack = dc + ":" + rack
    @tokens = tokens.sort
  end

  def merge!(node)
    raise "unmatched node: self=#{self} other=#{node}" if ip != node.ip || dc != node.dc || rack != node.rack
    @tokens.concat(node.tokens)
    self
  end

  def to_s
    "[Node##{object_id} ip: #{ip}, dc: #{dc}, rack: #{rack}, tokens: [#{tokens.size}...]]"
  end
end

def add_or_merge(hash, key, node)
  if existing_node = hash[key]
    existing_node.merge!(node)
  else
    hash[key] = node
  end
end

puts "calculating..."

nodes_by_address = {}
ring_output_file = ARGV[0]
contains_leaving_nodes = false
current_dc = ""
ring_output = File.readlines(ring_output_file)
ring_output.each do |line|
  next if line =~ /^$|^=|^Address|^ *\d+ *$|^ *(Note|Warning):|^ *To view status/
  if line =~ /^Datacenter: (.*)/
    current_dc = $1
    next
  end
  if line =~ /Leaving/
    contains_leaving_nodes = true
    next
  end
  if line =~ /([^ ]+) +([^ ]+) .* (-?\d+) *$/
    ip = $1
    rack = $2
    token = $3.to_i
    node = Node.new(ip, current_dc, rack, [ token ])

    add_or_merge(nodes_by_address, node.ip, node)
    next
  end
  raise "unrecognized line: #{line}"
end

options_file = File.join(File.dirname(ring_output_file), File.basename(ring_output_file) + ".options")
options = JSON.parse(File.read(options_file)) rescue {}
nodes_by_address.each_value.map(&:dc).uniq.each do |dc|
  rf = options[dc]["rf"] rescue nil
  if not rf
    print "DC: #{dc}, Enter replication factor (aka RF, default 3): "
    rf = $stdin.gets.chomp
    rf = rf.size > 0 && rf.to_i || 3
    print "DC: #{dc}, Enter node count to decommission (per-rack, default 2): "
    decommission_count = $stdin.gets.chomp
    decommission_count = decommission_count.size > 0 && decommission_count.to_i || 2
    options[dc] = {}
    options[dc]["rf"] = rf
    options[dc]["decommission_count"] = decommission_count
  end
end
File.write(options_file, JSON.pretty_generate(options))

nodes_by_dc_and_rack = Hash.new([])
nodes_by_address.each_value do |node|
  nodes_by_dc_and_rack[node.dc_and_rack] += [ node ]
end

nodes_by_token = Hash.new([])
nodes_by_address.each_value do |node|
  nodes_by_token.merge!(Hash[node.tokens.zip([node].cycle).sort_by{|token, node| -token}])
end

puts
puts "node count: #{nodes_by_address.size}"
puts "rack count: #{nodes_by_dc_and_rack.size}"
puts "rack descr:"
nodes_by_dc_and_rack.each do |dc_and_rack, nodes|
  puts "- #{dc_and_rack}: #{nodes.size}"
end
puts "token count: #{nodes_by_token.size}"
puts "config:\n#{JSON.pretty_generate(options)}"

def force_64_bit_overflow(n)
  ((n + 2**63) % 2**64) - 2**63
end

def count_tokens_in_token_sets(token_sets)
  token_sets.each_cons(2).reduce(0) do |count, ((start_token, node), (end_token, _))|
    count + force_64_bit_overflow(end_token - start_token)
  end
end

def count_tokens(node_to_count, nodes_by_token, replication_factor)
  wraparound = nodes_by_token.select{|token, node| node.dc == node_to_count.dc}.reverse.uniq{|token, node| node.rack}.reverse
  nodes_with_wraparound = nodes_by_token + wraparound
  replica_sets = []
  nodes_by_token.each do |token, node|
    next if not node.equal?(node_to_count)
    i = nodes_with_wraparound.rindex{|t, n| t == token}
    end_ = [ i+10, nodes_with_wraparound.size ].min
    replica_sets << nodes_with_wraparound[i..end_].select{|token, node| node.dc == node_to_count.dc}.uniq{|token, node| node.rack}
  end
  replica_sets.reduce(0) do |count, replica_set|
    count + count_tokens_in_token_sets(replica_set)
  end
end

def calculate_token_stddev_without_node(nodes_by_token, node_to_remove, replication_factor)
  nodes_by_token.delete_if{|token| node_to_remove.tokens.include?(token)}
  debug "after removing #{node_to_remove.ip}"
  nodes_in_rack = nodes_by_token.map{|toke, node| node}.select{|node| node.dc_and_rack == node_to_remove.dc_and_rack}
  token_counts = nodes_in_rack.map do |node|
    token_count = count_tokens(node, nodes_by_token, replication_factor)  # don't count tokens using only the rack
    debug "node #{node.ip} has total tokens: #{token_count}"
    token_count
  end

  sum = token_counts.sum
  mean = token_counts.sum / token_counts.size.to_f
  debug "mean: #{mean}"
  total_squares_from_mean = token_counts.reduce(0) do |total, token_count|
    total + (token_count - mean) ** 2
  end
  variance = total_squares_from_mean / token_counts.size.to_f
  stddev = Math.sqrt(variance)
  debug "stddev without #{node_to_remove.ip} is #{stddev}"
  stddev
end

def calculate_best_nodes_to_decommission(nodes_by_token, node_permutations, replication_factor)
  stddevs_per_permutation = []
  permutation_size = node_permutations[0].size
  dc = node_permutations[0][0].dc
  node_permutations.each_with_index do |permutation, i|
    nodes = Array[*nodes_by_token.select{|token, node| dc == node.dc}]
    debug "\ntrying permutation: #{permutation.map(&:ip).inspect}"
    stddevs = permutation.map do |node|
      calculate_token_stddev_without_node(nodes, node, replication_factor)
    end
    stddevs_per_permutation[i] = stddevs.sum / stddevs.size.to_f
  end

  least_stddev_index = stddevs_per_permutation.index(stddevs_per_permutation.min)
  least_stddev = stddevs_per_permutation[least_stddev_index]
  first_preferred_ips = node_permutations[least_stddev_index].map(&:ip)

  next_preferred_stddev = nil
  stddevs_per_permutation.each_with_index do |sum, i|
    next if node_permutations[i].find{|node| first_preferred_ips.include? node.ip}
    next_preferred_stddev = [ next_preferred_stddev || sum+1, sum ].min
  end
  next_least_stddev_index = stddevs_per_permutation.index(next_preferred_stddev)
  next_least_stddev = stddevs_per_permutation[next_least_stddev_index]
  next_preferred_ips = node_permutations[next_least_stddev_index].map(&:ip)
  [ first_preferred_ips, next_preferred_ips, least_stddev, next_least_stddev ]
end

def format_ip_list(ips)
  if ips.size < 2
    ips.join
  else
    "first #{ips.join ', then '}"
  end
end

puts
puts "decommission plan:"
nodes_by_dc_and_rack.each do |dc_and_rack, nodes|
  dc = nodes[0].dc
  nodes_to_decommission = options[dc]["decommission_count"]
  replication_factor = options[dc]["replication_factor"]
  next if nodes_to_decommission == 0
  if nodes.size > nodes_to_decommission
    node_permutations = nodes.permutation(nodes_to_decommission).to_a
    best_nodes_to_decommission,
    next_best_nodes_to_decommission,
    least_stddev,
    next_least_stddev = calculate_best_nodes_to_decommission(nodes_by_token, node_permutations, replication_factor)
    puts "=> %s: %s [%.2e] (next best: %s [%.2e])" % [
      dc_and_rack,
      format_ip_list(best_nodes_to_decommission),
      least_stddev,
      format_ip_list(next_best_nodes_to_decommission),
      next_least_stddev
    ]
    debug "\n*****************************************************************************"
  else
    puts "=> #{dc_and_rack}: unable to remove #{nodes_to_decommission} nodes from this rack"
  end
end

if contains_leaving_nodes
  puts
  puts "WARNING: ring output contains leaving nodes\nWait until decommission is complete before decommissioning further nodes!!"
end
