require 'rsolr'

class Ingester
  attr_accessor :solr
  attr_accessor :filepath
  def initialize(filepath)
    @filepath = filepath
  end
  def solr
    @solr ||= RSolr.connect :url => 'http://127.0.0.1:9999/solr/lcsh-core'
  end

  def ingest
    File.open(filepath, "r") do |handle|
      terms =[]
      handle.each_line do |triple|
        parts = /^<(.+?)> <http:\/\/www.w3.org\/2004\/02\/skos\/core#prefLabel> "(.*)"@en/.match(triple)
        terms << term(parts[1], parts[2]) if parts
        if terms.count == 1000
          index(terms)
          terms = []
        end
      end
    end
  end

  def term(id, label)
    {:id => id, :label => label}
  end

  def commit
    solr.commit
  end

  def index(terms)
   solr.add terms
   solr.commit
  end
end

ingester = Ingester.new("./subjects.nt")
ingester.ingest