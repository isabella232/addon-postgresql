using Apprenda.SaaSGrid.Addons;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npsql;

namespace Apprenda.PostgreSQL.AddOn
{
    public class PostgreSQLAddOn : AddonBase
    {
        const string ConnectionStringFormatter = @"Server={0};Port={1};User Id={2};Password={3};Database={4};";

        static class Queries
        {
            public const string CreateUser = @"CREATE USER {0} WITH LOGIN PASSWORD '{1}';";
            public const string CreateDatabase = @"CREATE DATABASE {0} WITH OWNER {1};";
            public const string GrantAllProvilegesToDatabase = @"GRANT ALL PRIVILEGES ON DATABASE ""{0}"" TO {1};";
        }

        static class Keys 
        {
            public const string Server = "pgsqlServer";
            public const string Port = "pgsqlServerPort";
            public const string AdminUser = "pgsqlAdminUser";
            public const string AdminPassword = "pgsqlAdminPassword";
        }

        private string Server { get; set; }
        private int Port { get; set; }
        private string Database { get; set; }
        private string AdminUserId { get; set; }
        private string AdminPassword { get; set; }
        private string ConnectionString { get { return string.Format(ConnectionStringFormatter, Server, Port, AdminUserId, AdminPassword, Database); } }
        
        public override OperationResult Deprovision(AddonDeprovisionRequest request)
        {
            ParseProperties(request.Manifest.Properties);
        }

        public override ProvisionAddOnResult Provision(AddonProvisionRequest request)
        {
            ParseProperties(request.Manifest.Properties);
        }

        public override OperationResult Test(AddonTestRequest request)
        {
            ParseProperties(request.Manifest.Properties);
            Npgsql.NpgsqlConnection
        }

        private 

        private void ParseProperties(List<AddonProperty> properties)
        {
            try
            {
                Server = properties.Find(p => p.Key == Keys.Server).Value;
                Port = properties.Find(p => p.Key == Keys.Port).Value;
                AdminUserId = properties.Find(p => p.Key == Keys.AdminUser).Value;
                AdminPassword = properties.Find(p => p.Key == Keys.AdminPassword).Value;
                Database = properties.Find(p => p.Key == Keys.Database).Value;
            }
            catch (Exception ex)
            {
                //TODO: Log failure...
            }
        }

    }
}
