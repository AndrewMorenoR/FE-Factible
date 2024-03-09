using Business;
using Business.Soap;
using Log;
using Model.Data;
using Model.Query;
using Ninject.Modules;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WSSendXmlToSoap
{
    class DependencyInjection : NinjectModule
    {
        public override void Load()
        {
            this.Bind<IEventLogStore>().To<EventLogStore>().InSingletonScope();
            this.Bind<IGeneralProcess>().To<GeneralProcess>().InSingletonScope();
            this.Bind<ISoapProcess>().To<SoapProcess>().InSingletonScope();
            this.Bind<IXmlProcess>().To<XmlProcess>().InSingletonScope();
            this.Bind<IAnticiposGeneration>().To<AnticiposGeneration>().InSingletonScope();
            this.Bind<IDbQuery>().To<DbQuery>().InSingletonScope();
            this.Bind<IDescuentosGeneration>().To<DescuentosGeneration>().InSingletonScope();
            this.Bind<IImpuestosGeneration>().To<ImpuestosGeneration>().InSingletonScope();
            this.Bind<IItemsGeneration>().To<ItemsGeneration>().InSingletonScope();
            this.Bind<IGeneralDataGeneration>().To<GeneralDataGeneration>().InSingletonScope();
            this.Bind<IAdjuntosGeneration>().To<AdjuntosGeneration>().InSingletonScope();
        }
    }
}
