package com.jd.bdp.fpmc

import com.jd.bdp.fpmc.entity.origin.FeatureDescription
import com.jd.bdp.fpmc.tools.MakeExamples.LabelMapping
import com.jd.bdp.fpmc.tools.SparkSql2HbaseFeature.{OneFeatureMapping, CrossFeatureMapping}
import spray.json._

/**
 * Created by zhengchen on 2015/9/15.
 */
package object tools {

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val featureDescriptionFormat = jsonFormat4(FeatureDescription.apply)

    /*implicit object FeatureDescriptionJsonFormat extends RootJsonFormat[FeatureDescription] {
      def write(c: FeatureDescription) =
        JsArray(JsString(c.vwName), JsString(c.description), JsString(c.user), JsString(c.date))

      def read(value: JsValue) = value match {
        case JsArray(Vector(JsString(vwName), JsString(description), JsString(user), JsString(date))) =>
          new FeatureDescription(vwName, description, user, date)
        case _ => deserializationError("FeatureDescription expected")
      }
    }*/

    implicit val crossFeatureMappingFormat = jsonFormat4(CrossFeatureMapping.apply)
    implicit val oneFeatureMappingFormat = jsonFormat2(OneFeatureMapping.apply)
    implicit val labelMappingFormat = jsonFormat5(LabelMapping.apply)

  }


}
