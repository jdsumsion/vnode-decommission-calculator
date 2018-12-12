#!/usr/bin/env ruby

require 'json'
require 'pp'

def usage_and_exit
  $stderr.puts "usage: #{File.basename($0)} <nodetool-ring.out>"
  exit 1
end

unless ARGV.size >= 1 and File.exist? ARGV[0]
  usage_and_exit
end

###
# For extra debug output, set DEBUG=true
DEBUG=ARGV.include?("--debug")

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
  decommission_count = options[dc]["decommission_count"] rescue nil
  if not decommission_count
    print "DC: #{dc}, Enter node count to decommission (per-rack, default 2): "
    decommission_count = $stdin.gets.chomp
    decommission_count = decommission_count.size > 0 && decommission_count.to_i || 2
    options[dc] = {}
    options[dc]["decommission_count"] = decommission_count
  end
end
File.write(options_file, JSON.pretty_generate(options))

nodes_by_dc_and_rack = Hash.new([])
nodes_by_address.each_value do |node|
  nodes_by_dc_and_rack[node.dc_and_rack] += [ node ]
end

puts
puts "node count: #{nodes_by_address.size}"
puts "rack count: #{nodes_by_dc_and_rack.size}"
puts "rack descr:"
nodes_by_dc_and_rack.each do |dc_and_rack, nodes|
  puts "- #{dc_and_rack}: #{nodes.size}"
end
puts "token count: #{nodes_by_address.each_value.map(&:tokens).flatten.size}"
puts "config:\n#{JSON.pretty_generate(options)}"

def force_64_bit_overflow(n)
  ((n + 2**63) % 2**64) - 2**63
end

def count_tokens_in_token_sets(node_to_count, token_sets)
  token_sets.each_cons(2).reduce(0) do |count, ((start_token, _), (end_token, node))|
    count + (node_to_count.equal?(node) && force_64_bit_overflow(end_token - start_token) || 0)
  end
end

def count_tokens(node_to_count, nodes_in_rack)
  nodes_with_wraparound = Array[nodes_in_rack[-1], *nodes_in_rack]
  token_sets = nodes_with_wraparound.map{|node| node.tokens.zip([node].cycle)}.flatten(1).sort_by{|token, node| token}
  count_tokens_in_token_sets(node_to_count, token_sets)
end

def calculate_token_score_without_node(nodes_in_rack, node_to_remove)
  nodes_in_rack.delete_if{|node| node_to_remove.equal?(node)}
  debug "after removing #{node_to_remove.ip}"
  token_counts = nodes_in_rack.map do |node|
    token_count = count_tokens(node, nodes_in_rack)
    debug "node #{node.ip} has total tokens: #{token_count}"
    token_count
  end

  max2 = token_counts.sort.reverse[0..2].sum
  median = token_counts.sort[token_counts.size / 2]
  skew = ((token_counts.sum / token_counts.size.to_f) - median).abs
  score = max2 - median + skew
  debug "score: #{score}"
  score
end

def calculate_best_nodes_to_decommission(nodes_in_rack, node_permutations)
  scores_per_permutation = []
  node_permutations.each_with_index do |permutation, i|
    nodes = Array[*nodes_in_rack]
    debug "\ntrying permutation: #{permutation.map(&:ip).inspect}"
    scores = permutation.map do |node|
      calculate_token_score_without_node(nodes, node)
    end
    scores_per_permutation[i] = scores.sum / scores.size.to_f
  end

  least_score_index = scores_per_permutation.index(scores_per_permutation.min)
  least_score = scores_per_permutation[least_score_index]
  first_preferred_ips = node_permutations[least_score_index].map(&:ip)

  next_preferred_score = nil
  scores_per_permutation.each_with_index do |sum, i|
    next if node_permutations[i].find{|node| first_preferred_ips.include? node.ip}
    next_preferred_score = [ next_preferred_score || sum+1, sum ].min
  end
  next_least_score_index = scores_per_permutation.index(next_preferred_score)
  next_least_score = scores_per_permutation[next_least_score_index]
  next_preferred_ips = node_permutations[next_least_score_index].map(&:ip)
  [ first_preferred_ips, next_preferred_ips, least_score, next_least_score ]
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
nodes_by_dc_and_rack.each do |dc_and_rack, nodes_in_rack|
  dc = nodes_in_rack[0].dc
  decommission_count = options[dc]["decommission_count"]
  next if decommission_count == 0
  if nodes_in_rack.size > decommission_count
    node_permutations = nodes_in_rack.permutation(decommission_count).to_a
    best_nodes_to_decommission,
    next_best_nodes_to_decommission,
    least_score,
    next_least_score = calculate_best_nodes_to_decommission(nodes_in_rack, node_permutations)
    puts "=> %s: %s [%.2e] (next best: %s [%.2e])" % [
      dc_and_rack,
      format_ip_list(best_nodes_to_decommission),
      least_score,
      format_ip_list(next_best_nodes_to_decommission),
      next_least_score
    ]
    debug "\n*****************************************************************************"
  else
    puts "=> #{dc_and_rack}: unable to remove #{decommission_count} nodes from this rack"
  end
end

if contains_leaving_nodes
  puts
  puts "WARNING: ring output contains leaving nodes\nWait until decommission is complete before decommissioning further nodes!!"
end
