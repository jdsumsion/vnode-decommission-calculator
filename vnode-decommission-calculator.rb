#!/usr/bin/env ruby

def usage_and_exit
  $stderr.puts "usage: #{File.basename($0)} <nodetool-ring.out> [nodes-to-remove-from-each-rack]"
  exit 1
end

unless ARGV.size >= 1 and File.exist? ARGV[0]
  usage_and_exit
end

nodes_to_remove_from_each_rack = 2
if ARGV.size > 1
  unless ARGV[1] =~ /^\d+$/
    usage_and_exit
  end
  nodes_to_remove_from_each_rack = ARGV[1].to_i
end

class Node

  attr_accessor :ip, :dc, :rack, :tokens

  def initialize(ip, dc, rack, tokens=[])
    @ip = ip
    @dc = dc
    @rack = rack
    @tokens = tokens
  end

  def dc_and_rack
    @dc + ":" + @rack
  end

  def merge!(node)
    raise "unmatched node: self=#{self} other=#{node}" if ip != node.ip or dc != node.dc or rack != node.rack
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
nodes_by_token = {}
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
    nodes_by_token[token] = node  # unique tokens means no merge
    next
  end
  raise "unrecognized line: #{line}"
end

nodes_by_rack = Hash.new([])
nodes_by_address.each_value do |node|
  nodes_by_rack[node.dc_and_rack] += [ node ]
end

puts
puts "nodes to remove from each rack: #{nodes_to_remove_from_each_rack}"
puts "node count: #{nodes_by_address.size}"
puts "rack count: #{nodes_by_rack.size}"
puts "rack descr:"
nodes_by_rack.each do |rack, nodes|
  puts "- #{rack}: #{nodes.size}"
end
puts "token count: #{nodes_by_token.size}"

def calculate_token_mean_variance_without_node(nodes, ip)
  nodes.delete_if{|node| ip == node.ip}
  remaining_tokens = nodes.map(&:tokens).flatten
  mean = remaining_tokens.reduce(:+) / remaining_tokens.size.to_f
  mean_variance = 0.0
  remaining_tokens.each do |token|
    mean_variance += (token - mean).abs
  end
  #puts "mean_variance without #{ip} is #{mean_variance}"
  mean_variance
end

def calculate_best_token_distribution(all_nodes, node_permutations)
  variance_sums_per_permutation = []
  permutation_size = node_permutations[0].size
  node_permutations.each_with_index do |permutation, i|
    nodes = Array[*all_nodes]
    mean_variance_sum = 0
    #puts "permutation: #{permutation.map(&:ip).join ','}"
    permutation.each do |node|
      mean_variance_sum += calculate_token_mean_variance_without_node(nodes, node.ip)
    end
    variance_sums_per_permutation[i] = mean_variance_sum
  end

  least_variance_index = variance_sums_per_permutation.index(variance_sums_per_permutation.min)
  first_preferred_ips = node_permutations[least_variance_index].map(&:ip)

  next_preferred_variance = nil
  variance_sums_per_permutation.each_with_index do |sum, i|
    next if node_permutations[i].find{|node| first_preferred_ips.include? node.ip}
    next_preferred_variance = [ next_preferred_variance || sum+1, sum ].min
  end
  next_least_variance_index = variance_sums_per_permutation.index(next_preferred_variance)
  next_preferred_ips = node_permutations[next_least_variance_index].map(&:ip)
  [ first_preferred_ips, next_preferred_ips ]
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
nodes_by_rack.each do |rack, nodes|
  if nodes.size > nodes_to_remove_from_each_rack
    node_permutations = nodes.permutation(nodes_to_remove_from_each_rack).to_a
    best_nodes_to_decommission, next_best_nodes_to_decommission = calculate_best_token_distribution(nodes, node_permutations)
    puts "=> #{rack}: #{format_ip_list(best_nodes_to_decommission)} (next best: #{format_ip_list(next_best_nodes_to_decommission)})"
  else
    puts "=> #{rack}: unable to remove #{nodes_to_remove_from_each_rack} nodes from this rack"
  end
end

if contains_leaving_nodes
  puts
  puts "WARNING: ring output contains leaving nodes\nWait until decommission is complete before decommissioning further nodes!!"
end
