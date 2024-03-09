using Model.XmlModel;
using System;
using System.Collections.Generic;

namespace Model.Data
{
    public interface IAdjuntosGeneration
    {
        /// <summary>
		/// Genera un listado de objetos del tipo XmlIAdjunto tomando como entrada la respuesta del query
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlItem los cuales son los Items asignados a los documentos a procesar  </returns>
        List<XmlAdjunto> GenerateAttachmentsList();
    }
}
