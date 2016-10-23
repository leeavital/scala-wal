/**
  * Convert both ways between I and O.
  */
trait Codec[I, O] {
  def serialize(in: I): O

  def deserialize(in: O): I
}
