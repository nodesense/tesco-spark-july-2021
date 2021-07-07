package workshop

object Application  {
  def DATA_DIR = "/home/krish/data"

  def getPath(path: String) = DATA_DIR + "/" + path
}
