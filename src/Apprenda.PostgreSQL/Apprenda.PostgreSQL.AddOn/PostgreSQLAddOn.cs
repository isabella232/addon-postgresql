using Apprenda.SaaSGrid.Addons;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using System.Security.Cryptography;
using System.Data;
using Apprenda.Services.Logging;

namespace Apprenda.PostgreSQL.AddOn
{
    public class PostgreSQLAddOn : AddonBase
    {
        const string ConnectionStringFormatter = @"Server={0};Port={1};User Id={2};Password={3};Database={4};";
        const string DatabaseNameFormatter = @"{0}__{1}";
        const string DatabaseUsernameFormatter = @"DB_{0}__{1}";

        private static readonly ILogger log = LogManager.Instance().GetLogger(typeof(PostgreSQLAddOn));

        static class Queries
        {
            public const string CreateUser = @"CREATE USER {0} WITH LOGIN PASSWORD '{1}';";
            public const string CreateDatabase = @"CREATE DATABASE {0} WITH OWNER {1};";
            public const string GrantAllPrivilegesToDatabase = @"GRANT ALL PRIVILEGES ON DATABASE ""{0}"" TO {1};";
            public const string DropDatabase = @"DROP DATABASE IF EXISTS {0};";
            public const string DropUser = @"DROP USER IF EXISTS {0}";
        }

        static class Keys 
        {
            public const string Server = "pgsqlServer";
            public const string Port = "pgsqlServerPort";
            public const string AdminDatabase = "pgsqlAdminDatabase";
            public const string AdminUser = "pgsqlAdminUser";
            public const string AdminPassword = "pgsqlAdminPassword";
        }

        private string Server { get; set; }
        private int Port { get; set; }
        private string AdminDatabase { get; set; }
        private string AdminUserId { get; set; }
        private string AdminPassword { get; set; }
        private string AdminConnectionString { get { return string.Format(ConnectionStringFormatter, Server, Port, AdminUserId, AdminPassword, AdminDatabase); } }
        private string NewDatabase { get; set; }
        private string NewUserId { get; set; }
        private string NewPassword { get; set; }
        private string NewConnectionString { get { return string.Format(ConnectionStringFormatter, Server, Port, NewUserId, NewPassword, NewDatabase); } }
        public override OperationResult Deprovision(AddonDeprovisionRequest request)
        {
            var result = new OperationResult();
            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);

                log.InfoFormat("Removing PostgreSQL database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    DropDatabase(connection);

                    DropUser(connection);
                }

                result.IsSuccess = true;
                result.EndUserMessage = "Successfully removed a PostgreSQL database.";

                log.InfoFormat("Successfully removed PostgreSQL database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to remove PostgreSQL database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        public override ProvisionAddOnResult Provision(AddonProvisionRequest request)
        {
            var result = new ProvisionAddOnResult();
            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);
                NewPassword = GetPassword();

                log.InfoFormat("Creating PostgreSQL database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    CreateUser(connection);

                    CreateDatabase(connection);

                    GrantPrivileges(connection);
                }

                result.IsSuccess = true;
                result.ConnectionData = NewConnectionString;
                result.EndUserMessage = "Successfully created a PostgreSQL database.";

                log.InfoFormat("Successfully created PostgreSQL database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.ConnectionData = "";
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to create PostgreSQL database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        public override OperationResult Test(AddonTestRequest request)
        {
            var result = new OperationResult();

            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);
                NewPassword = GetPassword();

                log.InfoFormat("Creating and removing PostgreSQL database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    CreateUser(connection);

                    CreateDatabase(connection);

                    GrantPrivileges(connection);

                    DropDatabase(connection);

                    DropUser(connection);
                }

                result.IsSuccess = true;
                result.EndUserMessage = "Successfully created and removed a PostgreSQL database.";

                log.InfoFormat("Successfully created and removed PostgreSQL database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to create or remove PostgreSQL database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        private void DropDatabase(NpgsqlConnection connection)
        {
            var dropDatabaseCommand = connection.CreateCommand();
            dropDatabaseCommand.CommandText = string.Format(Queries.DropDatabase, NewDatabase);
            ExecuteCommand(dropDatabaseCommand);
        }

        private void DropUser(NpgsqlConnection connection)
        {
            var dropUserCommand = connection.CreateCommand();
            dropUserCommand.CommandText = string.Format(Queries.DropUser, NewUserId);
            ExecuteCommand(dropUserCommand);
        }

        private void GrantPrivileges(NpgsqlConnection connection)
        {
            var grantPrivilegesCommand = connection.CreateCommand();
            grantPrivilegesCommand.CommandText = string.Format(Queries.GrantAllPrivilegesToDatabase, NewDatabase, NewUserId);
            ExecuteCommand(grantPrivilegesCommand);
        }

        private void CreateDatabase(NpgsqlConnection connection)
        {
            var createDatabaseCommand = connection.CreateCommand();
            createDatabaseCommand.CommandText = string.Format(Queries.CreateDatabase, NewDatabase, NewUserId);
            ExecuteCommand(createDatabaseCommand);
        }

        private void CreateUser(NpgsqlConnection connection)
        {
            var createUserCommand = connection.CreateCommand();
            createUserCommand.CommandText = string.Format(Queries.CreateUser, NewUserId, NewPassword);
            ExecuteCommand(createUserCommand);
        }

        private static void ExecuteCommand(NpgsqlCommand command)
        {
            command.CommandType = CommandType.Text;
            command.ExecuteNonQuery();
        }

        private static string GetDatabaseName(AddonManifest manifest)
        {
            var developmentTeamAlias = manifest.CallingDeveloperAlias;
            var instanceAlias = manifest.InstanceAlias;

            return string.Format(DatabaseNameFormatter, developmentTeamAlias, instanceAlias);
        }

        private static string GetNewUsername(AddonManifest manifest)
        {
            var developmentTeamAlias = manifest.CallingDeveloperAlias;
            var instanceAlias = manifest.InstanceAlias;

            return string.Format(DatabaseUsernameFormatter, developmentTeamAlias, instanceAlias);
        }

        private static string GetPassword()
        {
            var guid = Guid.NewGuid();
            var now = DateTime.Now;
            var inputString = now.ToLongTimeString() + "__" + guid.ToString();
            
            byte[] bytes = Encoding.Unicode.GetBytes(inputString);
            SHA256Managed sha256 = new SHA256Managed();
            byte[] hash = sha256.ComputeHash(bytes);
            string password = string.Empty;
            foreach (byte x in hash)
            {
                password += String.Format("{0:x2}", x);
            }
            return password;
        }

        private NpgsqlConnection GetConnection(List<AddonProperty> properties)
        {
            ParseProperties(properties);

            return GetConnection(AdminConnectionString);
        }

        private NpgsqlConnection GetConnection(string connectionString)
        {
            var connection = new NpgsqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        private void ParseProperties(List<AddonProperty> properties)
        {
            try
            {
                Server = properties.Find(p => p.Key == Keys.Server).Value;
                Port = int.Parse(properties.Find(p => p.Key == Keys.Port).Value);
                AdminUserId = properties.Find(p => p.Key == Keys.AdminUser).Value;
                AdminPassword = properties.Find(p => p.Key == Keys.AdminPassword).Value;
                AdminDatabase = properties.Find(p => p.Key == Keys.AdminDatabase).Value;
            }
            catch (Exception ex)
            {
                //TODO: Log failure...
            }
        }

    }
}
