package projects.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import projects.entity.Category;
import projects.entity.Material;
import projects.entity.Project;
import projects.entity.Step;
import provided.util.DaoBase;
import projects.exception.DbException;

public class ProjectDao extends DaoBase {
	private static final String CATEGORY_TABLE = "category";
	private static final String MATERIAL_TABLE = "material";
	private static final String PROJECT_TABLE = "project";
	private static final String PROJECT_CATEGORY_TABLE = "project_category";
	private static final String STEP_TABLE = "step";

	public Project insertProject(Project project) {
		// @formatter:off

		String sql = ""
			+ "INSERT INTO " + PROJECT_TABLE + " "
			+ "(project_name, estimated_hours, actual_hours, difficulty, notes)"
			+ "VALUES "
			+ "(?, ?, ?, ?, ?)";
		// @formatter:on

	
		try (Connection conn = DbConnection.getConnection()) {
			startTransaction(conn);


			try (PreparedStatement statement = conn.prepareStatement(sql)) {
				setParameter(statement, 1, project.getProjectName(), String.class);
				setParameter(statement, 2, project.getEstimatedHours(), BigDecimal.class);
				setParameter(statement, 3, project.getActualHours(), BigDecimal.class);
				setParameter(statement, 4, project.getDifficulty(), Integer.class);
				setParameter(statement, 5, project.getNotes(), String.class);

			
				statement.executeUpdate();
				Integer projectId = getLastInsertId(conn, PROJECT_TABLE);

				commitTransaction(conn);

				project.setProjectId(projectId);

				return project;

			} catch (Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}
		
		} catch (SQLException e) {
			throw new DbException(e);
		}
	}

	private List<Category> fetchCategoriesForProject(Connection conn, Integer projectId) throws SQLException {
		// @formatter:off
		String sql = "SELECT c.* FROM " + CATEGORY_TABLE + " c " 
					+ "JOIN " + PROJECT_CATEGORY_TABLE + " pc USING (category_id) "
					+ "WHERE project_id = ?";
		// @formatter:on 

		try (PreparedStatement statement = conn.prepareStatement(sql)) {
			setParameter(statement, 1, projectId, Integer.class);

			try (ResultSet resultSet = statement.executeQuery()) {
				List<Category> categories = new LinkedList<>();

			
				while (resultSet.next()) {
					categories.add(extract(resultSet, Category.class));
				}

				return categories;
			}
		}
	}

	public List<Project> fetchAllProjects() {
		String sql = "SELECT * FROM " + PROJECT_TABLE + " ORDER BY project_name";

		try (Connection conn = DbConnection.getConnection()) {
			startTransaction(conn);

			try (PreparedStatement statement = conn.prepareStatement(sql)) {

				try (ResultSet resultSet = statement.executeQuery()) {
					List<Project> projects = new LinkedList<>();

					while (resultSet.next()) {
						projects.add(extract(resultSet, Project.class)); // Adds each object to the projects list
					}

					return projects;

				}
			} catch (Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}
		} catch (SQLException e) {
			throw new DbException(e);
		}
	}

	public Optional<Project> fetchProjectById(Integer projectId) {
		String sql = "SELECT * FROM " + PROJECT_TABLE + " WHERE project_id = ?";

		try (Connection conn = DbConnection.getConnection()) {
			startTransaction(conn);

			try {
				Project project = null;

				try (PreparedStatement statement = conn.prepareStatement(sql)) {
					setParameter(statement, 1, projectId, Integer.class);

					try (ResultSet resultSet = statement.executeQuery()) {
						if (resultSet.next()) {
							project = extract(resultSet, Project.class);

						}
					}
				}

				if (Objects.nonNull(project)) {
					project.getMaterials().addAll(fetchMaterialsForProject(conn, projectId));
					project.getSteps().addAll(fetchStepsForProject(conn, projectId));
					project.getCategories().addAll(fetchCategoriesForProject(conn, projectId));
				}

				commitTransaction(conn);

				return Optional.ofNullable(project);

			} catch (Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}

		} catch (SQLException e) {
			throw new DbException(e);
		}

	}

	private List<Step> fetchStepsForProject(Connection conn, Integer projectId) throws SQLException {
		// @formatter:off
		String sql = "SELECT * FROM " + STEP_TABLE
				   + " WHERE project_id = ?";
		// @formatter:on 

		try (PreparedStatement statement = conn.prepareStatement(sql)) {
			setParameter(statement, 1, projectId, Integer.class);

			try (ResultSet resultSet = statement.executeQuery()) {
				List<Step> steps = new LinkedList<>();

				while (resultSet.next()) {
					steps.add(extract(resultSet, Step.class));
				}

				return steps;
			}
		}
	}

	private List<Material> fetchMaterialsForProject(Connection conn, Integer projectId) throws SQLException {
		// @formatter:off
		String sql = "SELECT * FROM " + MATERIAL_TABLE
				   + " WHERE project_id = ?";
		// @formatter:on 

		try (PreparedStatement statement = conn.prepareStatement(sql)) {
			setParameter(statement, 1, projectId, Integer.class);

			try (ResultSet resultSet = statement.executeQuery()) {
				List<Material> materials = new LinkedList<>();

				while (resultSet.next()) {
					materials.add(extract(resultSet, Material.class));
				}

				return materials;
			}
		}
	}

	public boolean modifyProjectDetails(Project project) {
		// @formatter:off
		String sql = "UPDATE " + PROJECT_TABLE + " SET "
				+ "project_name = ?, "
				+ "estimated_hours = ?, "
				+ "actual_hours = ?, "
				+ "difficulty = ?, "
				+ "notes = ? "
				+ "WHERE project_id = ?";
		// @formatter:on

		try (Connection conn = DbConnection.getConnection()) {
			startTransaction(conn);

			try (PreparedStatement statement = conn.prepareStatement(sql)) {
				setParameter(statement, 1, project.getProjectName(), String.class);
				setParameter(statement, 2, project.getEstimatedHours(), BigDecimal.class);
				setParameter(statement, 3, project.getActualHours(), BigDecimal.class);
				setParameter(statement, 4, project.getDifficulty(), Integer.class);
				setParameter(statement, 5, project.getNotes(), String.class);
				setParameter(statement, 6, project.getProjectId(), Integer.class);

				boolean updated = statement.executeUpdate() == 1;

				commitTransaction(conn);

				return updated;

			} catch (Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}

		} catch (SQLException e) {
			throw new DbException(e);
		}
	}

	public boolean deleteProject(int projectId) {
		String sql = "DELETE FROM " + PROJECT_TABLE + " WHERE project_id = ?";

		try (Connection conn = DbConnection.getConnection()) {
			startTransaction(conn);

			try (PreparedStatement statement = conn.prepareStatement(sql)) {
				setParameter(statement, 1, projectId, Integer.class);

				boolean deleted = statement.executeUpdate() == 1;

				commitTransaction(conn);

			} catch (Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}

		} catch (SQLException e) {
			throw new DbException(e);
		}
		return true;
	}
}
