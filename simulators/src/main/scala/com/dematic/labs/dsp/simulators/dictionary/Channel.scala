/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.simulators.dictionary

object Channel {
  val channel: Map[String, ChannelDefinition] = Map(
    "TruckForce_N" -> new ChannelDefinition(0, 50000, "N"),
    "TSteerAngle" -> new ChannelDefinition(-120, 120, "deg"),
    "TruckSpeed" -> new ChannelDefinition(-25, 25, "km/h"),
    "AxLoad" -> new ChannelDefinition(0, 20000, "kg"),
    "battery_temperature" -> new ChannelDefinition(-55, 155, "C" + "\u00b0"),
    "SpeedMinBackw" -> new ChannelDefinition(0, 25, "km/h"),
    "SpeedMinForw" -> new ChannelDefinition(0, 25, "km/h"),
    "EConsum" -> new ChannelDefinition(0, 50, "kWh/h"),
    "Load" -> new ChannelDefinition(0, 2000, "kg"),
    "SpLim" -> new ChannelDefinition(0, 0, ""),
    "Key2State" -> new ChannelDefinition(0, 0, ""),
    "Key1State" -> new ChannelDefinition(0, 0, ""),
    "KeyEncState" -> new ChannelDefinition(0, 0, ""),
    "EncState" -> new ChannelDefinition(0, 0, ""),
    "WorkTimeToBeDisplayed" -> new ChannelDefinition(0, 0, "sec"),
    "TruckWorkTime" -> new ChannelDefinition(0, 0, "sec"),
    "TracDirLev" -> new ChannelDefinition(0, 0, ""),
    "PermHeight" -> new ChannelDefinition(0, 0, "mm"),
    "SpLimState" -> new ChannelDefinition(0, 0, ""),
    "TrBrk" -> new ChannelDefinition(0, 0, ""),
    "RemWorkTime" -> new ChannelDefinition(0, 0, "min"),
    "BatTemp" -> new ChannelDefinition(-50, 120, "C" + "\u00b0"),
    "PowerSupply" -> new ChannelDefinition(0, 0, ""),
    "DaysToNextService" -> new ChannelDefinition(0, 0, "d"),
    "WorkTimeToNextService" -> new ChannelDefinition(0, 0, "min"),
    "L1_motTemp" -> new ChannelDefinition(0, 120, "C" + "\u00b0"),
    "LiftTiltAng" -> new ChannelDefinition(0, 8, "deg"),
    "LiftHeight" -> new ChannelDefinition(0, 25000, "mm"),
    "LiftActive" -> new ChannelDefinition(0, 0, ""),
    "WeighProc" -> new ChannelDefinition(0, 0, ""),
    "T_motTemp_Lft" -> new ChannelDefinition(0, 120, "C" + "\u00b0"),
    "T_BatCurr" -> new ChannelDefinition(0, 1000, "A"),
    "T_BatVolt" -> new ChannelDefinition(0, 130, "V"),
    "BatCharState" -> new ChannelDefinition(0, 100, "%"),
    "BrakeState" -> new ChannelDefinition(0, 0, ""),
    "T_TotalDischarge" -> new ChannelDefinition(0, 0, ""),
    "T_Seat" -> new ChannelDefinition(0, 0, ""),
    "BatVolt" -> new ChannelDefinition(0, 131000, "mV"),
    "FunctionKeyState" -> new ChannelDefinition(0, 0, ""),
    "ResetKeyState" -> new ChannelDefinition(0, 0, ""),
    "L1_estTorque" -> new ChannelDefinition(0, 250, "Nm"),
    "L1_measSpeed" -> new ChannelDefinition(0, 4500, "rpm"),
    "JoyAux4" -> new ChannelDefinition(0, 0, ""),
    "JoyAux3" -> new ChannelDefinition(0, 0, ""),
    "JoyAux2" -> new ChannelDefinition(0, 0, ""),
    "JoyTilt" -> new ChannelDefinition(0, 0, ""),
    "JoyAux1" -> new ChannelDefinition(0, 0, ""),
    "JoyLift" -> new ChannelDefinition(0, 0, ""),
    "DateTime" -> new ChannelDefinition(0, 0, "sec"),
    "LiIoBMS_BatteryVoltage" -> new ChannelDefinition(0, 130, "V"),
    "LiIoBMS_BatteryCurrent" -> new ChannelDefinition(0, 1000, "A"),
    "LiIonCharger_ChargingVoltV" -> new ChannelDefinition(0, 130, "V"),
    "LiIonCharger_ChargingCurrA" -> new ChannelDefinition(0, 1000, "A"),
    "LiIoBMS_SOCwithSOH" -> new ChannelDefinition(0, 100, "%"),
    "LiIoBMS_OutputVoltage" -> new ChannelDefinition(0, 130, "V"))

  def validate(channel: String, value: Long) {
  }
}

final class ChannelDefinition(val min: Long, val max: Long, val unit: String) {
  def getMin: Long = {
    min
  }

  def getMax: Long = {
    max
  }

  def getUnit: String = {
    unit
  }

  override def toString = s"ChannelDefinition($min, $max, $unit)"
}
