package link.rdcn.util

object ScalaExtensions {
  implicit class TapAny[A](private val self: A) extends AnyVal {
    @inline def tap(f: A => Unit): A = { f(self); self }
  }
}