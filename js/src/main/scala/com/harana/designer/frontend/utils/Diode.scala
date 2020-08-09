package com.harana.designer.frontend.utils

import diode.{Circuit, Dispatcher, ModelR}
import slinky.core.facade.Hooks.{useContext, useEffect, useState}
import slinky.core.facade.ReactContext

object Diode {

  def use[M <: AnyRef, T, S <: Circuit[M]](context: ReactContext[S], selector: M => T)(implicit feq: diode.FastEq[_ >: T]): (T, Dispatcher) = {
    val circuit = useContext(context)
    val applySelector = circuit.zoom(selector)
    val (state, setState) = useState[T](default = applySelector())

    useEffect(() => {
      val subscription = circuit.subscribe(applySelector)(state => setState(state.value))
      () => subscription()
    })

    (state, circuit)
  }


  def use[M <: AnyRef, T, S <: Circuit[M]](context: ReactContext[S], selector: ModelR[M, T]): (T, Dispatcher) = {
    val circuit = useContext(context)
    val (state, setState) = useState[T](default = selector())

    useEffect(() => {
      val subscription = circuit.subscribe(selector)(state => setState(state.value))
      () => subscription()
    })

    (state, circuit)
  }


  def use[Dispatcher](context: ReactContext[Dispatcher]): Dispatcher = {
    useContext(context)
  }

}