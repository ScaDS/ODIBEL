package ai.scads.odibel.utils

class DataDiff {


  def diff[A](left: Iterable[A], right: Iterable[A], equal: (A,A) => Boolean): (Iterable[A], Iterable[A]) = {
    val leftSet = left.toSet
    val rightSet = right.toSet
    val leftMinusRight = leftSet.diff(rightSet)
    val rightMinusLeft = rightSet.diff(leftSet)
    (leftMinusRight, rightMinusLeft)
  }
}

class DataDelta() {

//  def delta[A](left: Iterable[A], right: Iterable[A], equal: (A,A) => Boolean): Iterable[A] = {
//    val (leftMinusRight, rightMinusLeft) = diff(left, right, equal)
//    leftMinusRight ++ rightMinusLeft
//  }
}