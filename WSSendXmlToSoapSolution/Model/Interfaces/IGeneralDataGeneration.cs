using Model.XmlModel;
using System.Collections.Generic;

namespace Model.Data
{
    public interface IGeneralDataGeneration
    {
        /// <summary>
		/// Genera un listado de objetos del tipo Xmldocumento tomando como entrada la respuesta del query de documentos, y los listados de anticipos, Descuentos, Impuestos e Items
		/// </summary>
		/// <returns> Devuelve un listado de objetos Xmldocumento los cuales son los documentos a procesar  </returns>
        List<Xmldocumento> GenerateDocumentList();
    }
}